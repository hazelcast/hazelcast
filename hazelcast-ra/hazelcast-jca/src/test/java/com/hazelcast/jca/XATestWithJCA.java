/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jca;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class XATestWithJCA extends HazelcastTestSupport {

    private HazelcastConnection connection;
    private HazelcastInstance instance;

    @Before
    public void setup() throws Exception {
        instance = createHazelcastInstance();
        ResourceAdapterImpl resourceAdapter = new ResourceAdapterImpl();
        resourceAdapter.setHazelcastInstance(instance);
        ManagedConnectionFactoryImpl managedConnectionFactory = new ManagedConnectionFactoryImpl();
        managedConnectionFactory.setResourceAdapter(resourceAdapter);
        managedConnectionFactory.setConnectionTracingDetail(true);
        managedConnectionFactory.setConnectionTracingEvents("FACTORY_INIT, CREATE, TX_START, TX_COMPLETE, CLEANUP, DESTROY");
        TestConnectionManager connectionManager = new TestConnectionManager();
        ConnectionFactoryImpl factory = new ConnectionFactoryImpl(managedConnectionFactory, connectionManager);
        connection = factory.getConnection();
    }

    @Test
    public void testPut() throws ResourceException {
        String name = randomString();
        String key = randomString();
        String val = randomString();

        TransactionalMap<String, String> map = connection.getTransactionalMap(name);
        map.put(key, val);
        connection.close();

        IMap<String, String> m = instance.getMap(name);
        assertEquals(val, m.get(key));
    }

    @Test
    public void testTransactionalQueueShouldPollWhatWasOffered() throws ResourceException {
        String name = randomString();
        String item = randomString();

        TransactionalQueue<String> queue = connection.getTransactionalQueue(name);
        queue.offer(item);

        connection.close();

        IQueue<String> q = instance.getQueue(name);
        assertEquals(item,q.poll());
    }

    @Test
    public void testTransactionalMultiMapShouldHaveWhatIsInserted() throws ResourceException {
        String name = randomString();
        String key = randomString();
        String val1 = randomString();
        String val2 = randomString();

        TransactionalMultiMap<String, String> multiMap = connection.getTransactionalMultiMap(name);
        multiMap.put(key,val1);
        multiMap.put(key,val2);

        connection.close();

        MultiMap<String, String> m = instance.getMultiMap(name);
        Collection<String> vals = m.get(key);
        assertTrue(vals.contains(val1));
        assertTrue(vals.contains(val2));
        assertEquals(vals.size(), 2);
    }

    @Test
    public void testTransactionalListShouldHaveWhatIsInserted() throws ResourceException {
        String name = randomString();
        String item = randomString();

        TransactionalList<String> list = connection.getTransactionalList(name);
        list.add(item);

        connection.close();

        List<String> l = instance.getList(name);
        assertEquals(item,l.get(0));
    }


    @Test
    public void testTransactionalSetShouldHaveWhatIsInserted() throws ResourceException {
        String name = randomString();
        String item = randomString();

        TransactionalSet<String> set = connection.getTransactionalSet(name);
        set.add(item);

        connection.close();

        Set<String> s = instance.getSet(name);
        assertTrue(s.contains(item));
    }

    static class TestConnectionManager implements ConnectionManager, ConnectionEventListener {

        ConcurrentMap<HazelcastConnection, Xid> transactionIdMap = new ConcurrentHashMap<HazelcastConnection, Xid>();

        private static Xid createXid() {
            String s = randomString();
            return new SerializableXID(s.length(), s.getBytes(), s.getBytes());
        }

        @Override
        public Object allocateConnection(ManagedConnectionFactory mcf, ConnectionRequestInfo cxRequestInfo)
                throws ResourceException {
            ManagedConnection managedConnection = mcf.createManagedConnection(null, cxRequestInfo);
            XAResource xaResource = managedConnection.getXAResource();
            managedConnection.addConnectionEventListener(this);
            Xid xid = createXid();
            try {
                xaResource.start(xid, XAResource.TMNOFLAGS);
            } catch (XAException e) {
                throw ExceptionUtil.rethrow(e);
            }
            HazelcastConnection connection = (HazelcastConnection) managedConnection.getConnection(null, cxRequestInfo);
            transactionIdMap.put(connection, xid);
            return connection;
        }

        @Override
        public void connectionClosed(ConnectionEvent event) {
            ManagedConnection managedConnection = (ManagedConnection) event.getSource();
            HazelcastConnection connection = (HazelcastConnection) event.getConnectionHandle();
            Xid xid = transactionIdMap.remove(connection);
            try {
                XAResource xaResource = managedConnection.getXAResource();
                xaResource.end(xid, XAResource.TMSUCCESS);

                xaResource.commit(xid, true);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }

        }

        @Override
        public void localTransactionStarted(ConnectionEvent event) {

        }

        @Override
        public void localTransactionCommitted(ConnectionEvent event) {

        }

        @Override
        public void localTransactionRolledback(ConnectionEvent event) {

        }

        @Override
        public void connectionErrorOccurred(ConnectionEvent event) {

        }

    }

}
