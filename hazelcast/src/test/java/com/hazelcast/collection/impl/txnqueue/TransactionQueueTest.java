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

package com.hazelcast.collection.impl.txnqueue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
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
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionQueueTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Test
    public void testPromotionFromBackup() {
        Config config = getConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance owner = factory.newHazelcastInstance(config);
        HazelcastInstance backup = factory.newHazelcastInstance(config);
        String name = generateKeyOwnedBy(owner);

        TransactionContext context = backup.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue<VersionedObject<Integer>> queue = context.getQueue(name);
        queue.offer(new VersionedObject<>(1, 1));
        owner.getLifecycleService().terminate();
        queue.offer(new VersionedObject<>(2, 2));

        context.commitTransaction();
    }

    @Test
    public void testSingleQueueAtomicity() throws ExecutionException, InterruptedException {
        String name = randomString();
        int itemCount = 200;
        HazelcastInstance instance = createHazelcastInstance();

        Future<Integer> f = spawn(() -> {
            IQueue<VersionedObject<String>> queue = instance.getQueue(name);
            queue.take();
            return queue.size();
        });

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue<VersionedObject<String>> queue = context.getQueue(name);
        for (int i = 0; i < itemCount; i++) {
            queue.offer(new VersionedObject<>("item-" + i, i));
        }
        context.commitTransaction();

        int size = f.get();
        assertEquals(itemCount - 1, size);
    }

    @Test
    public void testOfferTake() throws InterruptedException {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance owner = factory.newHazelcastInstance(config);
        HazelcastInstance backup = factory.newHazelcastInstance(config);
        String name = generateKeyOwnedBy(owner);

        for (int i = 0; i < 1000; i++) {
            TransactionContext ctx = owner.newTransactionContext();
            ctx.beginTransaction();
            TransactionalQueue<VersionedObject<Integer>> queue = ctx.getQueue(name);
            queue.offer(new VersionedObject<>(1, 1));
            queue.take();
            ctx.commitTransaction();
        }
        assertEquals(0, owner.getQueue(name).size());

        assertTransactionMapSize(owner, name, 0);
        assertTransactionMapSize(backup, name, 0);
    }

    @Test
    public void testPeekWithTimeout() {
        String name = randomString();
        VersionedObject<String> item = new VersionedObject<>(randomString());
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<VersionedObject<String>> queue = instance.getQueue(name);
        spawn(() -> {
            sleepSeconds(1);
            queue.offer(item);
        });

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<VersionedObject<String>> txnQueue = context.getQueue(name);
            VersionedObject<String> peeked = txnQueue.peek(10, SECONDS);
            assertEquals(item, peeked);
            context.commitTransaction();
        } catch (Exception e) {
            context.rollbackTransaction();
        }
    }

    @Test
    public void testOrder_WhenMultipleConcurrentTransactionRollback() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        IQueue<VersionedObject<Integer>> queue = instance.getQueue(name);
        queue.offer(new VersionedObject<>(1, 1));
        queue.offer(new VersionedObject<>(2, 2));
        queue.offer(new VersionedObject<>(3, 3));

        TransactionContext firstContext = instance.newTransactionContext();
        firstContext.beginTransaction();
        firstContext.getQueue(name).poll();

        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            TransactionContext secondContext = instance.newTransactionContext();
            secondContext.beginTransaction();
            secondContext.getQueue(name).poll();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            secondContext.rollbackTransaction();
        });
        thread.start();
        firstContext.rollbackTransaction();
        latch.countDown();
        thread.join();

        assertEquals(new VersionedObject<>(1, 1), queue.poll());
        assertEquals(new VersionedObject<>(2, 2), queue.poll());
        assertEquals(new VersionedObject<>(3, 3), queue.poll());

    }

    @Test(expected = IllegalStateException.class)
    public void nestedTransactionTest() {
        HazelcastInstance instance = createHazelcastInstance();
        TransactionContext outerTnx = instance.newTransactionContext();
        outerTnx.beginTransaction();
        String name = randomString();
        VersionedObject<String> item = new VersionedObject<>(randomString());
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
    public void testTransactionalOfferPoll1() {
        int insCount = 4;
        String name = "defQueue";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        HazelcastInstance[] instances = factory.newInstances(getConfig());

        TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<VersionedObject<String>> q = context.getQueue(name);
            assertTrue(q.offer(new VersionedObject<>("ali")));
            VersionedObject<String> s = q.poll();
            assertEquals(new VersionedObject<>("ali"), s);
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
            throw e;
        }
        assertEquals(0, getQueue(instances, name).size());
    }

    @Test
    public void testTransactionalOfferPoll2() {
        int insCount = 4;
        String name0 = "defQueue0";
        String name1 = "defQueue1";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        HazelcastInstance[] instances = factory.newInstances(getConfig());
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                latch.await(5, SECONDS);
                sleepMillis(3000);
                getQueue(instances, name0).offer(new VersionedObject<>("item0"));
            } catch (InterruptedException | HazelcastInstanceNotActiveException ignored) {
            }
        }).start();

        TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<VersionedObject<String>> q0 = context.getQueue(name0);
        TransactionalQueue<VersionedObject<String>> q1 = context.getQueue(name1);
        VersionedObject<String> s = null;
        latch.countDown();
        try {
            s = q0.poll(10, SECONDS);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        assertEquals(new VersionedObject<>("item0"), s);
        q1.offer(s);
        context.commitTransaction();

        assertEquals(0, getQueue(instances, name0).size());
        assertEquals(new VersionedObject<>("item0"), getQueue(instances, name1).poll());
    }

    @Test
    public void testQueueWithMap() {
        int insCount = 4;
        String queueName = "defQueue";
        String mapName = "defMap";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        HazelcastInstance[] instances = factory.newInstances(getConfig());
        instances[0].getMap(mapName).lock("lock1");

        TransactionContext context = instances[1].newTransactionContext(new TransactionOptions().setTimeout(5, SECONDS));
        context.beginTransaction();
        try {
            boolean offered = context.getQueue(queueName).offer(new VersionedObject<>("item1"));
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
    public void testRollbackQueue() {
        HazelcastInstance h1 = createHazelcastInstance();

        TransactionContext transactionContext = h1.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalQueue<VersionedObject<String>> queue = transactionContext.getQueue("testq");
        queue.offer(new VersionedObject<>("offered-val"));
        transactionContext.rollbackTransaction();

        assertNull(h1.getQueue("testq").poll());

    }

    @Test(expected = TransactionNotActiveException.class)
    public void testTxnQueueOuterTransaction() {
        HazelcastInstance h1 = createHazelcastInstance();

        TransactionContext transactionContext = h1.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalQueue<VersionedObject<Object>> queue = transactionContext.getQueue("testTxnQueueOuterTransaction");
        queue.offer(new VersionedObject<>("item"));
        transactionContext.commitTransaction();
        queue.poll();
    }

    @Test
    public void testIssue859And863_WhenQueuesAreOnFirstInstance() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        String inQueueName = generateKeyOwnedBy(instance1);
        String outQueueName = generateKeyOwnedBy(instance1);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/11647#issuecomment-686505783")
    @Test
    public void testIssue859And863_WhenQueuesAreOnSecondInstance() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        String inQueueName = generateKeyOwnedBy(instance2);
        String outQueueName = generateKeyOwnedBy(instance2);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }

    @Test
    public void testIssue859And863_WhenInQueueOnFirstInstance_OutQueueOnSecondInstance() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        String inQueueName = generateKeyOwnedBy(instance1);
        String outQueueName = generateKeyOwnedBy(instance2);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/11647#issuecomment-686505783")
    @Test
    public void testIssue859And863_WhenInQueueOnSecondInstance_OutQueueOnFirstInstance() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        String inQueueName = generateKeyOwnedBy(instance2);
        String outQueueName = generateKeyOwnedBy(instance1);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }

    // https://github.com/hazelcast/hazelcast/issues/3796
    private void testIssue859And863(HazelcastInstance instance1,
                                    HazelcastInstance instance2,
                                    String inQueueName,
                                    String outQueueName) {
        int numberOfMessages = 3000;
        AtomicInteger counter = new AtomicInteger();

        IQueue<VersionedObject<Integer>> inQueue = instance1.getQueue(inQueueName);
        for (int i = 0; i < numberOfMessages; i++) {
            if (!inQueue.offer(new VersionedObject<>(i, i))) {
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
            assertTrueEventually(() -> {
                assertEquals(numberOfMessages, instance1.getQueue(outQueueName).size());
                assertTrue(instance1.getQueue(inQueueName).isEmpty());
            });
        } finally {
            interruptThreads(instance1Threads);
            interruptThreads(instance2Threads);
            assertJoinable(instance1Threads);
        }
    }

    private Thread[] createThreads(HazelcastInstance instance,
                                   int threadCount, String in, String out,
                                   AtomicInteger counter) {
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
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        String queueName = generateKeyOwnedBy(remote);

        // first we add an item
        local.executeTransaction(context -> {
            TransactionalQueue<VersionedObject<String>> queue = context.getQueue(queueName);
            queue.offer(new VersionedObject<>("item"));
            return null;
        });

        // we remove the item and then do a rollback. This causes the local (backup) to become out
        // of sync with the remote (primary)
        TransactionContext firstCtxt = local.newTransactionContext();
        firstCtxt.beginTransaction();
        TransactionalQueue<VersionedObject<String>> queue = firstCtxt.getQueue(queueName);
        queue.poll();
        firstCtxt.rollbackTransaction();

        // we kill the remote. Now the local (which was the backup) is going to become primary
        remote.shutdown();

        // if we take the item, we should get an error
        TransactionContext secondCtxt = local.newTransactionContext();
        secondCtxt.beginTransaction();
        queue = secondCtxt.getQueue(queueName);

        VersionedObject<String> found = queue.poll();
        assertEquals(new VersionedObject<>("item"), found);
    }

    @Test
    public void testPeekMethod() throws Exception {
        int insCount = 4;
        String name = "defQueue";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        HazelcastInstance[] instances = factory.newInstances(getConfig());

        TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<VersionedObject<String>> q = context.getQueue(name);
            VersionedObject<String> response1 = q.peek(10, SECONDS);
            assertNull(response1);

            assertTrue(q.offer(new VersionedObject<>("ali")));

            VersionedObject<String> response2 = q.peek();
            assertEquals(new VersionedObject<>("ali"), response2);
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
        VersionedObject<String> item = new VersionedObject<>("offered");
        String queueName = "testTransactionalOfferAndPollWithTimeout";

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<VersionedObject<String>> txnQueue = context.getQueue(queueName);
        assertTrue(txnQueue.offer(item));
        assertEquals(1, txnQueue.size());
        assertEquals(item, txnQueue.poll(5, SECONDS));
        context.commitTransaction();
    }

    @Test
    public void testPollWithTimeout_WithAnotherThreadOffering() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        CountDownLatch offerReserveLatch = new CountDownLatch(1);
        spawn(() -> {
            TransactionContext context = instance.newTransactionContext();
            context.beginTransaction();
            context.getQueue(name).offer(new VersionedObject<>(randomString()));
            offerReserveLatch.countDown();
            sleepAtLeastSeconds(2);
            context.commitTransaction();
        });
        assertOpenEventually(offerReserveLatch, 10);
        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<VersionedObject<String>> queue = context.getQueue(name);
        VersionedObject<String> item = queue.poll(30, SECONDS);
        assertNotNull(item);
        context.commitTransaction();
    }

    @Test
    public void transactionShouldBeRolledBack_whenInitiatorTerminatesBeforeCommit() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        warmUpPartitions(instance);

        String name = generateKeyOwnedBy(master);
        IQueue<VersionedObject<Integer>> queue = master.getQueue(name);
        queue.offer(new VersionedObject<>(1));

        waitAllForSafeState(master, instance);

        TransactionOptions options = new TransactionOptions()
                .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);

        TransactionContext context = master.newTransactionContext(options);
        context.beginTransaction();
        TransactionalQueue<VersionedObject<Integer>> txQueue = context.getQueue(name);
        txQueue.poll();

        master.getLifecycleService().terminate();

        IQueue<Integer> queue2 = instance.getQueue(name);
        assertTrueEventually(() -> assertEquals(1, queue2.size()));
        assertTrueAllTheTime(() -> assertEquals(1, queue2.size()), 3);
    }

    private void assertTransactionMapSize(HazelcastInstance instance, String name, int size) {
        QueueService queueService = getNode(instance).nodeEngine.getService(QueueService.SERVICE_NAME);
        assertEquals(size, queueService.getOrCreateContainer(name, true).txMapSize());
    }

    private <E> IQueue<E> getQueue(HazelcastInstance[] instances, String name) {
        Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getQueue(name);
    }

    static class MoveMessage implements Runnable {
        private final String inQueueName;
        private final String outQueueName;
        private final AtomicInteger counter;

        private final HazelcastInstance hazelcastInstance;

        MoveMessage(HazelcastInstance hazelcastInstance,
                    String inQueueName, String outQueueName,
                    AtomicInteger counter) {
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
                        Object item = transactionContext.getQueue(inQueueName).poll();
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
        HazelcastInstance hz = createHazelcastInstance();

        String name = randomName();
        IQueue<VersionedObject<String>> queue = hz.getQueue(name);

        EventCountingItemListener listener = new EventCountingItemListener();
        queue.addItemListener(listener, true);

        hz.executeTransaction((TransactionalTask<Object>) ctx -> {
            TransactionalQueue<VersionedObject<Object>> queue1 = ctx.getQueue(name);
            return queue1.offer(new VersionedObject<>("item"));
        });

        assertTrueEventually(() -> assertEquals(1, listener.adds.get()));
    }

    @Test
    public void testListener_withPoll() {
        HazelcastInstance hz = createHazelcastInstance();

        String name = randomName();
        IQueue<VersionedObject<String>> queue = hz.getQueue(name);
        queue.offer(new VersionedObject<>("item"));

        EventCountingItemListener listener = new EventCountingItemListener();
        queue.addItemListener(listener, true);

        VersionedObject<String> item = hz.executeTransaction(ctx -> {
            TransactionalQueue<VersionedObject<String>> queue1 = ctx.getQueue(name);
            return queue1.poll();
        });
        assertEquals(new VersionedObject<>("item"), item);

        assertTrueEventually(() -> assertEquals(1, listener.removes.get()));
    }

    @Test
    public void testListener_withEmptyPoll() {
        HazelcastInstance hz = createHazelcastInstance();

        String name = randomName();
        IQueue<VersionedObject<String>> queue = hz.getQueue(name);

        EventCountingItemListener listener = new EventCountingItemListener();
        queue.addItemListener(listener, true);

        VersionedObject<String> item = hz.executeTransaction(ctx -> {
            TransactionalQueue<VersionedObject<String>> queue1 = ctx.getQueue(name);
            return queue1.poll();
        });
        assertNull(item);

        assertTrueAllTheTime(() -> assertEquals(0, listener.removes.get()), 5);
    }

    private static class EventCountingItemListener implements ItemListener<VersionedObject<String>> {
        final AtomicInteger adds = new AtomicInteger();
        final AtomicInteger removes = new AtomicInteger();

        @Override
        public void itemAdded(ItemEvent<VersionedObject<String>> item) {
            adds.incrementAndGet();
        }

        @Override
        public void itemRemoved(ItemEvent<VersionedObject<String>> item) {
            removes.incrementAndGet();
        }
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        return config;
    }
}
