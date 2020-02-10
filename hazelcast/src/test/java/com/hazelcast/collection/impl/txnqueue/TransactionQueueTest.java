/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.test.Accessors.getNode;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionQueueTest extends HazelcastTestSupport {

    @Test
    public void testPromotionFromBackup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance owner = factory.newHazelcastInstance();
        HazelcastInstance backup = factory.newHazelcastInstance();
        String name = generateKeyOwnedBy(owner);

        TransactionContext context = backup.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue<Integer> queue = context.getQueue(name);
        queue.offer(1);
        owner.getLifecycleService().terminate();
        queue.offer(2);

        context.commitTransaction();
    }

    @Test
    public void testSingleQueueAtomicity() throws ExecutionException, InterruptedException {
        final String name = randomString();
        final int itemCount = 200;
        final HazelcastInstance instance = createHazelcastInstance();

        Future<Integer> f = spawn(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                IQueue<Object> queue = instance.getQueue(name);
                queue.take();
                return queue.size();
            }
        });

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue<Object> queue = context.getQueue(name);
        for (int i = 0; i < itemCount; i++) {
            queue.offer("item-" + i);
        }
        context.commitTransaction();

        int size = f.get();
        assertEquals(itemCount - 1, size);
    }

    @Test
    public void testOfferTake() throws ExecutionException, InterruptedException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance owner = factory.newHazelcastInstance();
        final HazelcastInstance backup = factory.newHazelcastInstance();
        final String name = generateKeyOwnedBy(owner);

        for (int i = 0; i < 1000; i++) {
            TransactionContext ctx = owner.newTransactionContext();
            ctx.beginTransaction();
            TransactionalQueue<Integer> queue = ctx.getQueue(name);
            queue.offer(1);
            queue.take();
            ctx.commitTransaction();
        }
        assertEquals(0, owner.getQueue(name).size());

        assertTransactionMapSize(owner, name, 0);
        assertTransactionMapSize(backup, name, 0);
    }


    @Test
    public void testPeekWithTimeout() {
        final String name = randomString();
        final String item = randomString();
        HazelcastInstance instance = createHazelcastInstance();
        final IQueue<String> queue = instance.getQueue(name);
        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(1);
                queue.offer(item);
            }
        });

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<String> txnQueue = context.getQueue(name);
            String peeked = txnQueue.peek(10, SECONDS);
            assertEquals(item, peeked);
            context.commitTransaction();
        } catch (Exception e) {
            context.rollbackTransaction();
        }
    }

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
                    latch.await(5, SECONDS);
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
            s = q0.poll(10, SECONDS);
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

        final TransactionContext context = instances[1].newTransactionContext(new TransactionOptions().setTimeout(5, SECONDS));
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
    public void testIssue859And863_WhenQueuesAreOnFirstInstance() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        String inQueueName = generateKeyOwnedBy(instance1);
        String outQueueName = generateKeyOwnedBy(instance1);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }

    @Test
    public void testIssue859And863_WhenQueuesAreOnSecondInstance() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        String inQueueName = generateKeyOwnedBy(instance2);
        String outQueueName = generateKeyOwnedBy(instance2);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }

    @Test
    public void testIssue859And863_WhenInQueueOnFirstInstance_OutQueueOnSecondInstance() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        String inQueueName = generateKeyOwnedBy(instance1);
        String outQueueName = generateKeyOwnedBy(instance2);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }

    @Test
    public void testIssue859And863_WhenInQueueOnSecondInstance_OutQueueOnFirstInstance() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        String inQueueName = generateKeyOwnedBy(instance2);
        String outQueueName = generateKeyOwnedBy(instance1);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }

    // https://github.com/hazelcast/hazelcast/issues/3796
    private void testIssue859And863(final HazelcastInstance instance1, final HazelcastInstance instance2,
                                    final String inQueueName, final String outQueueName) throws Exception {
        final int numberOfMessages = 3000;
        final AtomicInteger counter = new AtomicInteger();

        final IQueue<Object> inQueue = instance1.getQueue(inQueueName);
        for (int i = 0; i < numberOfMessages; i++) {
            if (!inQueue.offer(i)) {
                throw new RuntimeException("initial put did not work");
            }
        }
        Thread[] instance1Threads = createThreads(instance1, 3, inQueueName, outQueueName, counter);
        Thread[] instance2Threads = createThreads(instance2, 3, inQueueName, outQueueName, counter);

        try {
            startThreads(instance1Threads);
            startThreads(instance2Threads);

            while (counter.get() < numberOfMessages / 2) {
                LockSupport.parkNanos(1000);
            }
            instance2.getLifecycleService().shutdown();

            interruptThreads(instance2Threads);

            assertJoinable(instance2Threads);

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
            interruptThreads(instance1Threads);
            interruptThreads(instance2Threads);
            assertJoinable(instance1Threads);
        }
    }

    private Thread[] createThreads(HazelcastInstance instance, int threadCount, String in, String out, AtomicInteger counter) {
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            MoveMessage moveMessage = new MoveMessage(instance, in, out, counter);
            threads[i] = new Thread(moveMessage);
        }
        return threads;
    }

    private void interruptThreads(Thread[] threads) {
        for (Thread thread : threads) {
            thread.interrupt();
        }
    }

    private void startThreads(Thread[] threads) {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    // https://github.com/hazelcast/hazelcast/issues/6259
    @Test
    public void issue_6259_backupNotRollingBackCorrectly() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        final String queueName = generateKeyOwnedBy(remote);

        // first we add an item
        local.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext context) throws TransactionException {
                TransactionalQueue<String> queue = context.getQueue(queueName);
                queue.offer("item");
                return null;
            }
        });

        // we remove the item and then do a rollback. This causes the local (backup) to become out
        // of sync with the remote (primary)
        TransactionContext firstCtxt = local.newTransactionContext();
        firstCtxt.beginTransaction();
        TransactionalQueue<String> queue = firstCtxt.getQueue(queueName);
        queue.poll();
        firstCtxt.rollbackTransaction();

        // we kill the remote. Now the local (which was the backup) is going to become primary
        remote.shutdown();

        // if we take the item, we should get an error
        TransactionContext secondCtxt = local.newTransactionContext();
        secondCtxt.beginTransaction();
        queue = secondCtxt.getQueue(queueName);

        String found = queue.poll();
        assertEquals("item", found);
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
            final String response1 = q.peek(10, SECONDS);
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
        HazelcastInstance instance = createHazelcastInstance();
        String item = "offered";
        String queueName = "testTransactionalOfferAndPollWithTimeout";

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<String> txnQueue = context.getQueue(queueName);
        assertTrue(txnQueue.offer(item));
        assertEquals(1, txnQueue.size());
        assertEquals(item, txnQueue.poll(5, SECONDS));
        context.commitTransaction();
    }

    @Test
    public void testPollWithTimeout_WithAnotherThreadOffering() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance();
        final String name = randomString();
        final CountDownLatch offerReserveLatch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                TransactionContext context = instance.newTransactionContext();
                context.beginTransaction();
                context.getQueue(name).offer(randomString());
                offerReserveLatch.countDown();
                sleepAtLeastSeconds(2);
                context.commitTransaction();
            }
        });
        assertOpenEventually(offerReserveLatch, 10);
        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<Object> queue = context.getQueue(name);
        Object item = queue.poll(30, SECONDS);
        assertNotNull(item);
        context.commitTransaction();
    }

    @Test
    public void transactionShouldBeRolledBack_whenInitiatorTerminatesBeforeCommit() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance instance = factory.newHazelcastInstance();
        warmUpPartitions(instance);

        String name = generateKeyOwnedBy(master);
        IQueue<Integer> queue = master.getQueue(name);
        queue.offer(1);

        waitAllForSafeState(master, instance);

        TransactionOptions options =
                new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);

        TransactionContext context = master.newTransactionContext(options);
        context.beginTransaction();
        TransactionalQueue txQueue = context.getQueue(name);
        txQueue.poll();

        master.getLifecycleService().terminate();

        final IQueue<Integer> queue2 = instance.getQueue(name);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, queue2.size());
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, queue2.size());
            }
        }, 3);
    }

    private void assertTransactionMapSize(HazelcastInstance instance, String name, int size) {
        final QueueService queueService = getNode(instance).nodeEngine.getService(QueueService.SERVICE_NAME);
        assertEquals(size, queueService.getOrCreateContainer(name, true).txMapSize());
    }

    private <E> IQueue<E> getQueue(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getQueue(name);
    }

    class MoveMessage implements Runnable {
        private final String inQueueName;
        private final String outQueueName;
        private final AtomicInteger counter;

        private final HazelcastInstance hazelcastInstance;

        MoveMessage(HazelcastInstance hazelcastInstance, String inQueueName, String outQueueName, AtomicInteger counter) {
            this.hazelcastInstance = hazelcastInstance;
            this.inQueueName = inQueueName;
            this.outQueueName = outQueueName;
            this.counter = counter;

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
                            counter.incrementAndGet();
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

    @Test
    public void testListener_withOffer() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();

        final String name = randomName();
        IQueue<Object> queue = hz.getQueue(name);

        final EventCountingItemListener listener = new EventCountingItemListener();
        queue.addItemListener(listener, true);

        hz.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext ctx) throws TransactionException {
                TransactionalQueue<Object> queue = ctx.getQueue(name);
                return queue.offer("item");
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.adds.get());
            }
        });
    }

    @Test
    public void testListener_withPoll() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();

        final String name = randomName();
        IQueue<Object> queue = hz.getQueue(name);
        queue.offer("item");

        final EventCountingItemListener listener = new EventCountingItemListener();
        queue.addItemListener(listener, true);

        Object item = hz.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext ctx) throws TransactionException {
                TransactionalQueue<Object> queue = ctx.getQueue(name);
                return queue.poll();
            }
        });
        assertEquals("item", item);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.removes.get());
            }
        });
    }

    @Test
    public void testListener_withEmptyPoll() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();

        final String name = randomName();
        IQueue<Object> queue = hz.getQueue(name);

        final EventCountingItemListener listener = new EventCountingItemListener();
        queue.addItemListener(listener, true);

        Object item = hz.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext ctx) throws TransactionException {
                TransactionalQueue<Object> queue = ctx.getQueue(name);
                return queue.poll();
            }
        });
        assertNull(item);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, listener.removes.get());
            }
        }, 5);
    }

    private static class EventCountingItemListener implements ItemListener<Object> {
        final AtomicInteger adds = new AtomicInteger();
        final AtomicInteger removes = new AtomicInteger();

        @Override
        public void itemAdded(ItemEvent<Object> item) {
            adds.incrementAndGet();
        }

        @Override
        public void itemRemoved(ItemEvent<Object> item) {
            removes.incrementAndGet();
        }
    }
}
