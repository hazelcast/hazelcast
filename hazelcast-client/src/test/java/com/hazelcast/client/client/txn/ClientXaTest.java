/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.client.txn;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientXaTest {

    static final Random random = new Random(System.currentTimeMillis());

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
    public void testRollbackAfterNodeShutdown() throws Exception {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        tm.begin();

        final TransactionContext context = client.newTransactionContext();
        final XAResource xaResource = context.getXaResource();
        final Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);

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
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient();

        final TransactionContext context1 = client1.newTransactionContext(TransactionOptions.getDefault().setDurability(2));
        final XAResource xaResource1 = context1.getXaResource();
        final MyXid myXid = new MyXid();
        xaResource1.start(myXid, 0);
        final TransactionalMap<Object, Object> map = context1.getMap("map");
        map.put("key", "value");
        xaResource1.prepare(myXid);
        client1.shutdown();

        assertNull(instance1.getMap("map").get("key"));

        final HazelcastInstance client2 = HazelcastClient.newHazelcastClient();
        final TransactionContext context2 = client2.newTransactionContext();
        final XAResource xaResource2 = context2.getXaResource();
        final Xid[] recover = xaResource2.recover(0);
        for (Xid xid : recover) {
            xaResource2.commit(xid, false);
        }

        assertEquals("value", instance1.getMap("map").get("key"));

        try {
            context1.rollbackTransaction(); //for setting ThreadLocal of unfinished transaction
        } catch (Throwable ignored) {
        }
    }

    @Test
    public void testIsSame() throws Exception {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        final XAResource resource1 = instance1.newTransactionContext().getXaResource();
        final XAResource resource2 = instance2.newTransactionContext().getXaResource();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final XAResource clientResource = client.newTransactionContext().getXaResource();
        assertTrue(clientResource.isSameRM(resource1));
        assertTrue(clientResource.isSameRM(resource2));
    }

    @Test
    public void testTimeoutSetting() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final XAResource resource = instance.newTransactionContext().getXaResource();
        final int timeout = 100;
        final boolean result = resource.setTransactionTimeout(timeout);
        assertTrue(result);
        assertEquals(timeout, resource.getTransactionTimeout());
        final MyXid myXid = new MyXid();
        resource.start(myXid,0);
        assertFalse(resource.setTransactionTimeout(120));
        assertEquals(timeout, resource.getTransactionTimeout());
        resource.commit(myXid,true);
    }


    public static class MyXid implements Xid {

        public int getFormatId() {
            return 42;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return "GlobalTransactionId".getBytes();
        }

        @Override
        public byte[] getBranchQualifier() {
            return "BranchQualifier".getBytes();
        }
    }


    @Test
    public void testParallel() throws Exception {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final int size = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        final CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    try {
                        txn(client);
                    } catch (Exception e) {
                        e.printStackTrace();
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
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        txn(client);
        txn(client);
        txn(client);
    }

    private void txn(HazelcastInstance instance) throws Exception {
        tm.begin();

        final TransactionContext context = instance.newTransactionContext();
        final XAResource xaResource = context.getXaResource();
        final Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);

        boolean error = false;
        try {
            final TransactionalMap m = context.getMap("m");
            m.put(random.nextInt(10), "value");
        } catch (Exception e) {
            e.printStackTrace();
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
        if (error)
            flag = XAResource.TMFAIL;
        for (XAResource resource : xaResource) {
            tx.delistResource(resource, flag);
        }

        if (error)
            tm.rollback();
        else
            tm.commit();

    }
}
