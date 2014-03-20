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

package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.*;

/**
 * @author ali 6/7/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnQueueTest {

    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTransactionalOfferPoll1() throws Exception {
        final String item = "offered";
        final String queueName = randomString();
        final IQueue queue = client.getQueue(queueName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue txnQueue = context.getQueue(queueName);

        txnQueue.offer(item);
        assertEquals(item, txnQueue.poll());

        context.commitTransaction();
        assertEquals(0, queue.size());
    }

    @Test
    public void testTransactionalQueueGetsOfferedItems() throws Exception {
        final String item = "offered1";
        final String queueName = randomString();
        final IQueue queue1 = client.getQueue(queueName);

        final CountDownLatch justBeforeBlocked = new CountDownLatch(1);

        new Thread() {
            public void run() {
                try {
                    justBeforeBlocked.await();
                    sleepSeconds(1);
                    queue1.offer(item);
                } catch (InterruptedException e) {
                    fail("failed"+e);
                }
            }
        }.start();


        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue txnQueue1 = context.getQueue(queueName);
        try {
            justBeforeBlocked.countDown();
            Object result = txnQueue1.poll(5, TimeUnit.SECONDS);

            assertEquals("TransactionalQueue should get item offered from client queue", item, result);

        } catch (InterruptedException e) {
            fail("TransactionalQueue did not get item offered from client queue" + e);
        }
        context.commitTransaction();
    }

    @Test
    public void testTransactionalPeek() throws Exception {
        final String item = "offered";
        final String queunName = randomString();
        final IQueue queue = client.getQueue(queunName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue txnQueue = context.getQueue(queunName);

        txnQueue.offer(item);
        assertEquals(item, txnQueue.peek());
        assertEquals(item, txnQueue.peek());

        context.commitTransaction();

        assertEquals(1, queue.size());
    }

    @Test
    public void testTransactionalOfferRoleBack() throws Exception {
        final String name = randomString();
        final IQueue queue = client.getQueue(name);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<String> qTxn = context.getQueue(name);

        qTxn.offer("ITEM");
        context.rollbackTransaction();

        assertEquals(0, queue.size());
    }

    @Test
    public void testTransactionalQueueSize() throws Exception {
        final String item = "offered";
        final String name = randomString();
        final IQueue queue = client.getQueue(name);

        queue.offer(item);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<String> txnQueue = context.getQueue(name);

        txnQueue.offer(item);
        assertEquals(2, txnQueue.size());

        context.rollbackTransaction();
    }
}
