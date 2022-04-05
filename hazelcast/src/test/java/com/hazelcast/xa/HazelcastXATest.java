/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.xa;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastXATest extends HazelcastTestSupport {

    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final ILogger LOGGER = Logger.getLogger(HazelcastXATest.class);

    private UserTransactionManager tm;

    public void cleanAtomikosLogs() {
        try {
            File currentDir = new File(".");
            final File[] tmLogs = currentDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.endsWith(".epoch") || name.startsWith("tmlog");
                }
            });
            for (File tmLog : tmLogs) {
                tmLog.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void init() throws SystemException {
        cleanAtomikosLogs();
        tm = new UserTransactionManager();
        tm.setTransactionTimeout(60);
    }

    @After
    public void cleanup() {
        tm.close();
        cleanAtomikosLogs();
    }

    @Test
    public void testRollback() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        HazelcastXAResource xaResource = instance.getXAResource();

        tm.begin();
        Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);
        TransactionContext context = xaResource.getTransactionContext();
        boolean error = false;
        try {
            TransactionalMap<String, String> map = context.getMap("m");
            map.put("key", "value");
            throw new RuntimeException("Exception for rolling back");
        } catch (Exception e) {
            error = true;
        } finally {
            close(error, xaResource);
        }

        assertNull(instance.getMap("m").get("key"));
    }

    @Test
    public void testRecovery() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();
        HazelcastInstance instance3 = factory.newHazelcastInstance();

        HazelcastXAResource xaResource = instance1.getXAResource();
        Xid myXid = new SerializableXID(42, "globalTransactionId".getBytes(), "branchQualifier".getBytes());
        xaResource.start(myXid, 0);
        TransactionContext context1 = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context1.getMap("map");
        map.put("key", "value");
        xaResource.prepare(myXid);
        instance1.shutdown();

        instance1 = factory.newHazelcastInstance();
        xaResource = instance1.getXAResource();
        Xid[] recovered = xaResource.recover(0);
        for (Xid xid : recovered) {
            xaResource.commit(xid, false);
        }

        assertEquals("value", instance2.getMap("map").get("key"));
    }

    /**
     * Start two nodes.
     * One node in a new thread prepares tx and shutdowns. Check if remaining node can recover tx or not.
     */
    @Test
    public void testRecovery_singleInstanceRemaining() throws XAException {
        final CountDownLatch nodeShutdownLatch = new CountDownLatch(1);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        // 1. Start TX from a new thread.
        startTX(instance1, nodeShutdownLatch);

        waitNodeToShutDown(nodeShutdownLatch);

        // 2. Try to recover TX from other node.
        recoverTX(instance2);

        IMap<String, String> map = instance2.getMap("map");
        String actualValue = map.get("key");
        assertEquals("value", actualValue);
    }

    private void waitNodeToShutDown(CountDownLatch nodeShutdownLatch) {
        assertOpenEventually(nodeShutdownLatch);
    }

    private void startTX(final HazelcastInstance instance, final CountDownLatch nodeShutdownLatch) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    HazelcastXAResource xaResource = instance.getXAResource();
                    Xid xid = new SerializableXID(42, "globalTransactionId".getBytes(), "branchQualifier".getBytes());
                    xaResource.start(xid, XAResource.TMNOFLAGS);
                    TransactionContext context = xaResource.getTransactionContext();
                    final TransactionalMap<Object, Object> map = context.getMap("map");
                    map.put("key", "value");
                    xaResource.prepare(xid);

                    instance.shutdown();

                    nodeShutdownLatch.countDown();

                } catch (XAException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void recoverTX(HazelcastInstance instance) throws XAException {
        HazelcastXAResource xaResource = instance.getXAResource();
        final Xid[] recovered = xaResource.recover(XAResource.TMNOFLAGS);
        for (Xid xid : recovered) {
            xaResource.commit(xid, false);
        }
    }

    @Test
    public void testIsSame() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();
        HazelcastXAResource resource1 = instance1.getXAResource();
        HazelcastXAResource resource2 = instance2.getXAResource();
        assertTrue(resource1.isSameRM(resource2));
    }

    @Test
    public void testParallel() throws Exception {
        final HazelcastInstance instance = createHazelcastInstance();
        // this is needed due to a racy bug in atomikos
        txn(instance);
        int size = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        final CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    try {
                        txn(instance);
                    } catch (Exception e) {
                        LOGGER.severe("Exception during txn", e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        assertOpenEventually(latch, 20);
        final IMap map = instance.getMap("m");
        for (int i = 0; i < 10; i++) {
            assertFalse(map.isLocked(i));
        }
    }

    @Test
    public void testSequential() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        int count = 100;
        for (int i = 0; i < count; i++) {
            txn(instance);
        }
    }

    private void txn(HazelcastInstance instance) throws Exception {
        HazelcastXAResource xaResource = instance.getXAResource();

        tm.begin();
        Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);

        boolean error = false;
        try {
            TransactionContext context = xaResource.getTransactionContext();
            TransactionalMap<Integer, String> map = context.getMap("m");
            map.put(RANDOM.nextInt(10), "value");
        } catch (Exception e) {
            LOGGER.severe("Exception during transaction", e);
            error = true;
        } finally {
            close(error, xaResource);
        }
    }

    private void close(boolean error, XAResource... xaResource) throws Exception {

        int flag = XAResource.TMSUCCESS;

        // get the current tx
        Transaction tx = tm.getTransaction();
        // closeConnection
        if (error) {
            flag = XAResource.TMFAIL;
        }
        for (XAResource resource : xaResource) {
            tx.delistResource(resource, flag);
        }

        if (error) {
            tm.rollback();
        } else {
            tm.commit();
        }
    }
}
