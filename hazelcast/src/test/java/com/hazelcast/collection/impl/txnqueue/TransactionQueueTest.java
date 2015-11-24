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

package com.hazelcast.collection.impl.txnqueue;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TransactionQueueTest extends HazelcastTestSupport {

    @Test
    public void testOrder_WhenMultipleConcurrentTransactionRollback() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance();
        final String name = randomString();
        IQueue<Integer> queue = instance.getQueue(name);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);

        TransactionContext firstContext = instance.newTransactionContext();
        firstContext.beginTransaction();
        firstContext.getQueue(name).poll();

        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                TransactionContext secondContext = instance.newTransactionContext();
                secondContext.beginTransaction();
                secondContext.getQueue(name).poll();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                secondContext.rollbackTransaction();
            }
        };
        thread.start();
        firstContext.rollbackTransaction();
        latch.countDown();
        thread.join();

        assertEquals(1, queue.poll().intValue());
        assertEquals(2, queue.poll().intValue());
        assertEquals(3, queue.poll().intValue());

    }

    @Test(expected = IllegalStateException.class)
    public void nestedTransactionTest() {
        final HazelcastInstance instance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        TransactionContext outerTnx = instance.newTransactionContext();
        outerTnx.beginTransaction();
        final String name = randomString();
        final String item = randomString();
        outerTnx.getQueue(name).offer(item);
        try {
            TransactionContext innerTnx = instance.newTransactionContext();
            innerTnx.beginTransaction();
            innerTnx.getQueue(name).offer(item);
            innerTnx.commitTransaction();
        } finally {
            outerTnx.rollbackTransaction();
        }
    }

    @Test
    public void testTransactionalOfferPoll1() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String name = "defQueue";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);


        final TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<String> q = context.getQueue(name);
            assertTrue(q.offer("ali"));
            String s = q.poll();
            assertEquals("ali", s);
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
            throw e;
        }
        assertEquals(0, getQueue(instances, name).size());
    }

    @Test
    public void testTransactionalOfferPoll2() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String name0 = "defQueue0";
        final String name1 = "defQueue1";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                try {
                    latch.await(5, TimeUnit.SECONDS);
                    sleepMillis(3000);
                    getQueue(instances, name0).offer("item0");
                } catch (InterruptedException ignored) {
                } catch (HazelcastInstanceNotActiveException ignored) {
                }
            }
        }).start();

        final TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<String> q0 = context.getQueue(name0);
        TransactionalQueue<String> q1 = context.getQueue(name1);
        String s = null;
        latch.countDown();
        try {
            s = q0.poll(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        assertEquals("item0", s);
        q1.offer(s);
        context.commitTransaction();

        assertEquals(0, getQueue(instances, name0).size());
        assertEquals("item0", getQueue(instances, name1).poll());
    }

    @Test
    public void testQueueWithMap() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String queueName = "defQueue";
        final String mapName = "defMap";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        instances[0].getMap(mapName).lock("lock1");

        final TransactionContext context = instances[1].newTransactionContext(new TransactionOptions().setTimeout(5, TimeUnit.SECONDS));
        context.beginTransaction();
        try {
            boolean offered = context.getQueue(queueName).offer("item1");
            assertTrue(offered);
            context.getMap(mapName).put("lock1", "value1");
            fail();
        } catch (TransactionException ex) {
            // expected
            context.rollbackTransaction();
        }
        assertEquals(0, instances[0].getQueue(queueName).size());
        assertNull(instances[0].getMap(mapName).get("lock1"));
    }

    @Test
    public void testRollbackQueue() throws Throwable {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance h1 = factory.newHazelcastInstance();

        final TransactionContext transactionContext = h1.newTransactionContext();

        transactionContext.beginTransaction();

        TransactionalQueue<String> queue = transactionContext.getQueue("testq");

        queue.offer("offered-val");

        transactionContext.rollbackTransaction();

        assertNull(h1.getQueue("testq").poll());

    }

    @Test(expected = TransactionNotActiveException.class)
    public void testTxnQueueOuterTransaction() throws Throwable {
        final HazelcastInstance h1 = createHazelcastInstance();

        final TransactionContext transactionContext = h1.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalQueue<Object> queue = transactionContext.getQueue("testTxnQueueOuterTransaction");
        queue.offer("item");
        transactionContext.commitTransaction();
        queue.poll();
    }

    @Test
    @Ignore // https://github.com/hazelcast/hazelcast/issues/3796
    public void testIssue859And863() throws Exception {
        final int numberOfMessages = 1000;
        final AtomicInteger count = new AtomicInteger();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance();
        final HazelcastInstance instance2 = factory.newHazelcastInstance();

        final String inQueueName = "in";
        final String outQueueName = "out";

        class MoveMessage implements Runnable {
            private final HazelcastInstance hazelcastInstance;

            MoveMessage(HazelcastInstance hazelcastInstance) {
                this.hazelcastInstance = hazelcastInstance;
            }

            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        TransactionContext transactionContext = hazelcastInstance.newTransactionContext();
                        transactionContext.beginTransaction();
                        try {
                            final Object item = transactionContext.getQueue(inQueueName).poll();
                            if (item != null && !transactionContext.getQueue(outQueueName).offer(item)) {
                                throw new RuntimeException("Out Queue wouldn't accept item");
                            }
                            transactionContext.commitTransaction();
                            if (item != null) {
                                count.incrementAndGet();
                            }
                        } catch (HazelcastInstanceNotActiveException e) {
                            throw e;
                        } catch (Exception e) {
                            try {
                                transactionContext.rollbackTransaction();
                            } catch (HazelcastInstanceNotActiveException ignored) {
                            }
                        }
                    } catch (HazelcastInstanceNotActiveException e) {
                        break;
                    }
                }
            }
        }
        final IQueue<Object> inQueue = instance1.getQueue(inQueueName);
        for (int i = 0; i < numberOfMessages; i++) {
            if (!inQueue.offer(i)) {
                throw new RuntimeException("initial put did not work");
            }
        }
        final Thread moveMessage1 = new Thread(new MoveMessage(instance1)),
                moveMessage2 = new Thread(new MoveMessage(instance2));
        try {
            moveMessage1.start();
            moveMessage2.start();

            while (count.get() < numberOfMessages / 2) {
                LockSupport.parkNanos(1000);
            }
            instance2.getLifecycleService().shutdown();
            moveMessage2.interrupt();
            moveMessage1.join(10000);
            moveMessage2.join(10000);

            // When a node goes down, backup of the transaction commits all prepared stated transactions
            // Since it relies on 'memberRemoved' event, it is async. That's why we should assert eventually
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(numberOfMessages, instance1.getQueue(outQueueName).size());
                    assertTrue(instance1.getQueue(inQueueName).isEmpty());
                }
            });
        } finally {
            moveMessage1.interrupt();
            moveMessage2.interrupt();
        }
    }

    @Test
    public void testPeekMethod() throws Exception {
        final Config config = new Config();
        final int insCount = 4;
        final String name = "defQueue";
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<String> q = context.getQueue(name);
            final String response1 = q.peek(10, TimeUnit.SECONDS);
            assertNull(response1);

            assertTrue(q.offer("ali"));

            final String response2 = q.peek();
            assertEquals("ali", response2);
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
            throw e;
        }
        assertEquals(1, getQueue(instances, name).size());
    }

    @Test
    public void testTransactionalOfferAndPollWithTimeout() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        final String item = "offered";
        final String queueName = "testTransactionalOfferAndPollWithTimeout";

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<String> txnQueue = context.getQueue(queueName);
        assertTrue(txnQueue.offer(item));
        assertEquals(1, txnQueue.size());
        assertEquals(item, txnQueue.poll(5, TimeUnit.SECONDS));
        context.commitTransaction();
    }


    private <E> IQueue<E> getQueue(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getQueue(name);
    }

}
