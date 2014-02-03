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

package com.hazelcast.jca;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 30/01/14
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class HazelcastXaTest {

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");

        // randomize multicast group...
        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);
    }

    static HazelcastInstance instance;
    static final Random random = new Random(System.currentTimeMillis());

    @BeforeClass
    public static void beforeClass() {
        instance = Hazelcast.newHazelcastInstance();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testParallel() throws Exception {

        final int size = 300;
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        final CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    try {
                        txn();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        final IMap m = instance.getMap("m");
        for (int i = 0; i < 10; i++) {
            assertFalse(m.isLocked(i));
        }
    }

    @Test
    public void testSequential() throws Exception {
        txn();
        txn();
        txn();
    }

    private void txn() throws Exception {
        final TransactionManager tm = new UserTransactionManager();
        tm.setTransactionTimeout(60);
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
        // retrieve or construct a TM handle
        TransactionManager tm = new UserTransactionManager();

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
