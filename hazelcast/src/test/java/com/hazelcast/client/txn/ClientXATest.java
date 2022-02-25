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

package com.hazelcast.client.txn;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.collection.IQueue;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientXATest {

    static final Random random = new Random(System.currentTimeMillis());
    static final ILogger logger = Logger.getLogger(ClientXATest.class);

    UserTransactionManager tm = null;

    public void cleanAtomikosLogs() {
        try {
            File currentDir = new File(".");
            final File[] tmLogs = currentDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    if (name.endsWith(".epoch") || name.startsWith("tmlog")) {
                        return true;
                    }
                    return false;
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
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() {
        tm.close();
        cleanAtomikosLogs();
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testRollbackOnTimeout() throws Exception {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        String name = randomString();
        IQueue<Object> queue = client.getQueue(name);
        queue.offer(randomString());

        HazelcastXAResource xaResource = client.getXAResource();

        tm.setTransactionTimeout(3);
        tm.begin();
        Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);
        TransactionContext context = xaResource.getTransactionContext();
        try {
            context.getQueue(name).take();
            sleepAtLeastSeconds(5);
            tm.commit();
            fail();
        } catch (RollbackException ignored) {
            // Transaction already rolled-back due to timeout, no need to call tm.rollback explicitly
        }
        assertEquals("Queue size should be 1", 1, queue.size());
    }

    @Test
    public void testWhenLockedOutOfTransaction() throws Exception {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<Object, Object> map = client.getMap("map");
        map.put("key", "value");
        HazelcastXAResource xaResource = client.getXAResource();

        tm.begin();
        Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);
        TransactionContext context = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> transactionalMap = context.getMap("map");
        if (map.tryLock("key")) {
            transactionalMap.remove("key");
        }
        tm.commit();
    }

    @Test
    public void testRollback() throws Exception {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        HazelcastXAResource xaResource = client.getXAResource();

        tm.begin();
        Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);
        TransactionContext context = xaResource.getTransactionContext();
        boolean error = false;
        try {
            final TransactionalMap m = context.getMap("m");
            m.put("key", "value");
            throw new RuntimeException("Exception for rolling back");
        } catch (Exception e) {
            error = true;
        } finally {
            close(error, xaResource);
        }

        assertNull(client.getMap("m").get("key"));
    }

    @Test
    public void testRecovery() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        HazelcastXAResource xaResource = instance.getXAResource();
        Xid myXid = new SerializableXID(42, "globalTransactionId".getBytes(), "branchQualifier".getBytes());
        xaResource.start(myXid, 0);
        TransactionContext context1 = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context1.getMap("map");
        map.put("key", "value");
        xaResource.prepare(myXid);
        instance.shutdown();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        HazelcastXAResource clientXaResource = client.getXAResource();
        Xid[] recovered = clientXaResource.recover(0);
        for (Xid xid : recovered) {
            clientXaResource.commit(xid, false);
        }
        assertEquals("value", client.getMap("map").get("key"));
    }

    @Test
    public void testIsSame() throws Exception {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        XAResource resource1 = instance1.getXAResource();
        XAResource resource2 = instance2.getXAResource();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        XAResource clientResource = client.getXAResource();
        assertTrue(clientResource.isSameRM(resource1));
        assertTrue(clientResource.isSameRM(resource2));
    }

    @Test
    public void testParallel() throws Exception {
        Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        // this is needed due to a racy bug in atomikos
        txn(client);
        int size = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        final CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    try {
                        txn(client);
                    } catch (Exception e) {
                        logger.severe("Exception during txn", e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        assertOpenEventually(latch, 20);
        final IMap m = client.getMap("m");
        for (int i = 0; i < 10; i++) {
            assertFalse(m.isLocked(i));
        }
    }

    @Test
    public void testSequential() throws Exception {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        int count = 100;
        for (int i = 0; i < count; i++) {
            txn(client);
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
            TransactionalMap m = context.getMap("m");
            m.put(random.nextInt(10), "value");
        } catch (Exception e) {
            logger.severe("Exception during transaction", e);
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
