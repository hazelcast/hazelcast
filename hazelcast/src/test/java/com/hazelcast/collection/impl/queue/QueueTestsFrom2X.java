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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalQueue;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueTestsFrom2X extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        return config;
    }

    @Test
    public void testQueueItemListener() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<VersionedObject<String>> queue = instance.getQueue("testQueueItemListener");

        VersionedObject<String> value = new VersionedObject<>("hello");
        CountDownLatch latch = new CountDownLatch(8);
        queue.addItemListener(new ItemListener<VersionedObject<String>>() {
            public void itemAdded(ItemEvent<VersionedObject<String>> itemEvent) {
                assertEquals(value, itemEvent.getItem());
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<VersionedObject<String>> itemEvent) {
                assertEquals(value, itemEvent.getItem());
                latch.countDown();
            }
        }, true);

        queue.offer(value);
        assertEquals(value, queue.poll());
        queue.offer(value);
        assertTrue(queue.remove(value));
        queue.add(value);
        assertEquals(value, queue.remove());
        queue.put(value);
        assertEquals(value, queue.take());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testQueueAddAll() {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<VersionedObject<String>> queue = instance.getQueue("testQueueAddAll");

        @SuppressWarnings("unchecked")
        VersionedObject<String>[] items = new VersionedObject[]{
                new VersionedObject<>("one"),
                new VersionedObject<>("two"),
                new VersionedObject<>("three"),
                new VersionedObject<>("four")};

        queue.addAll(asList(items));
        assertEquals(4, queue.size());

        queue.addAll(asList(items));
        assertEquals(8, queue.size());
    }

    @Test
    public void testQueueContains() {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<VersionedObject<String>> queue = instance.getQueue("testQueueContains");

        @SuppressWarnings("unchecked")
        VersionedObject<String>[] items = new VersionedObject[]{
                new VersionedObject<>("one"),
                new VersionedObject<>("two"),
                new VersionedObject<>("three"),
                new VersionedObject<>("four")};
        queue.addAll(asList(items));

        assertContains(queue, new VersionedObject<>("one"));
        assertContains(queue, new VersionedObject<>("two"));
        assertContains(queue, new VersionedObject<>("three"));
        assertContains(queue, new VersionedObject<>("four"));
    }

    @Test
    public void testQueueContainsAll() {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<VersionedObject<String>> queue = instance.getQueue("testQueueContainsAll");

        List<VersionedObject<String>> items = Arrays.asList(
                new VersionedObject<>("one"),
                new VersionedObject<>("two"),
                new VersionedObject<>("three"),
                new VersionedObject<>("four"));
        queue.addAll(items);

        assertContainsAll(queue, items);
    }

    @Test
    public void testQueueRemove() {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<VersionedObject<String>> q = instance.getQueue("testQueueRemove");

        for (int i = 0; i < 10; i++) {
            q.offer(new VersionedObject<>("item" + i, i));
        }
        for (int i = 0; i < 5; i++) {
            assertNotNull(q.poll());
        }
        assertEquals(new VersionedObject<>("item5", 5), q.peek());

        boolean removed = q.remove(new VersionedObject<>("item5", 5));
        assertTrue(removed);

        Iterator<VersionedObject<String>> it = q.iterator();
        int i = 6;
        while (it.hasNext()) {
            int itemId = i++;
            assertEquals(new VersionedObject<>("item" + itemId, itemId), it.next());
        }
        assertEquals(4, q.size());
    }

    @Test
    @Ignore
    public void testInterruption() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IQueue<VersionedObject<String>> queue = instance.getQueue("testInterruption");
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                queue.poll(5, TimeUnit.SECONDS);
                fail();
            } catch (InterruptedException e) {
                latch.countDown();
            }
        });

        thread.start();
        sleep(2000);
        thread.interrupt();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        queue.offer(new VersionedObject<>("message"));
        assertEquals(1, queue.size());
    }

    @Test
    public void issue370() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance h2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(h1, h2);

        Queue<VersionedObject<String>> q1 = h1.getQueue("q");
        Queue<VersionedObject<String>> q2 = h2.getQueue("q");

        for (int i = 0; i < 5; i++) {
            q1.offer(new VersionedObject<>("item" + i, i));
        }
        assertEquals(5, q1.size());
        assertEquals(5, q2.size());
        assertEquals(new VersionedObject<>("item0", 0), q2.poll());
        assertEquals(new VersionedObject<>("item1", 1), q2.poll());
        assertEquals(new VersionedObject<>("item2", 2), q2.poll());
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());

        h1.shutdown();
        assertEquals(2, q2.size());

        HazelcastInstance h3 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(h2, h3);
        Queue<VersionedObject<String>> q3 = h3.getQueue("q");

        assertEquals(2, q2.size());
        assertEquals(2, q3.size());
        h2.shutdown();
        assertEquals(2, q3.size());
    }

    @Test
    public void issue391() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        int total = 10;
        Collection<VersionedObject<Integer>> results = new ArrayList<>(5);
        HazelcastInstance hz1 = factory.newHazelcastInstance(getConfig());
        CountDownLatch latchOffer = new CountDownLatch(1);
        CountDownLatch latchTake = new CountDownLatch(1);

        spawn(() -> {
            try {
                IQueue<VersionedObject<Integer>> q = hz1.getQueue("q");
                for (int i = 0; i < total; i++) {
                    results.add(q.take());
                }
                latchTake.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        HazelcastInstance hz2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(hz1, hz2);

        spawn(() -> {
            for (int i = 0; i < total; i++) {
                hz2.getQueue("q").offer(new VersionedObject<>(i, i));
            }
            latchOffer.countDown();
        });
        assertTrue(latchOffer.await(100, TimeUnit.SECONDS));
        assertTrue(latchTake.await(10, TimeUnit.SECONDS));
        assertTrue(hz1.getQueue("q").isEmpty());
        hz1.shutdown();
        assertTrue(hz2.getQueue("q").isEmpty());

        @SuppressWarnings("unchecked")
        VersionedObject<Integer>[] objects = new VersionedObject[total];
        for (int i = 0; i < total; i++) {
            objects[i] = new VersionedObject<>(i, i);
        }
        assertArrayEquals(objects, results.toArray());
    }

    @Test
    public void issue427QOfferIncorrectWithinTransaction() {
        Config config = getConfig();
        config.getQueueConfig("default").setMaxSize(100);
        HazelcastInstance instance = createHazelcastInstance(config);
        TransactionContext transactionContext = instance.newTransactionContext();
        transactionContext.beginTransaction();

        TransactionalQueue<VersionedObject<Integer>> queue = transactionContext.getQueue("default");
        for (int i = 0; i < 100; i++) {
            queue.offer(new VersionedObject<>(i, i));
        }
        boolean result = queue.offer(new VersionedObject<>(100, 100));
        assertEquals(100, queue.size());
        transactionContext.commitTransaction();

        assertEquals(100, instance.getQueue("default").size());
        assertFalse(result);
        instance.shutdown();
    }

    @Test
    public void testListenerLifecycle() throws Exception {
        long sleep = 2000;
        String name = "listenerLifecycle";

        HazelcastInstance instance = createHazelcastInstance();
        IQueue<VersionedObject<Integer>> queue = instance.getQueue(name);

        try {
            CountDownLatch latch = new CountDownLatch(3);
            ItemListener<VersionedObject<Integer>> listener = new ItemListener<VersionedObject<Integer>>() {
                public void itemAdded(ItemEvent<VersionedObject<Integer>> itemEvent) {
                    latch.countDown();
                }

                public void itemRemoved(ItemEvent<VersionedObject<Integer>> itemEvent) {
                }
            };

            queue.addItemListener(listener, false);
            queue.offer(new VersionedObject<>(1));
            sleep(sleep);
            queue.destroy();
            queue = instance.getQueue(name);
            UUID id = queue.addItemListener(listener, false);
            queue.offer(new VersionedObject<>(2));
            sleep(sleep);
            queue.removeItemListener(id);
            queue.offer(new VersionedObject<>(3));
            sleep(sleep);
            assertEquals(1, latch.getCount());

            latch.countDown();
            assertTrue("Remaining: " + latch.getCount(), latch.await(3, TimeUnit.SECONDS));
        } finally {
            queue.destroy();
        }
    }

    @Test
    public void testQueueOfferCommitSize() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);

        TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue<VersionedObject<String>> txnQ1 = context.getQueue("testQueueOfferCommitSize");
        TransactionalQueue<VersionedObject<String>> txnQ2 = context.getQueue("testQueueOfferCommitSize");
        txnQ1.offer(new VersionedObject<>("item"));
        assertEquals(1, txnQ1.size());
        assertEquals(1, txnQ2.size());

        context.commitTransaction();
        assertEquals(1, instance1.getQueue("testQueueOfferCommitSize").size());
        assertEquals(1, instance2.getQueue("testQueueOfferCommitSize").size());
        assertEquals(new VersionedObject<>("item"), instance2.getQueue("testQueueOfferCommitSize").poll());
    }

    @Test
    public void testQueueOfferRollbackSize() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);

        TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue<VersionedObject<String>> txnQ1 = context.getQueue("testQueueOfferRollbackSize");
        TransactionalQueue<VersionedObject<String>> txnQ2 = context.getQueue("testQueueOfferRollbackSize");

        txnQ1.offer(new VersionedObject<>("item"));
        assertEquals(1, txnQ1.size());
        assertEquals(1, txnQ2.size());

        context.rollbackTransaction();
        assertEquals(0, instance1.getQueue("testQueueOfferRollbackSize").size());
        assertEquals(0, instance2.getQueue("testQueueOfferRollbackSize").size());
    }

    @Test
    public void testQueuePollCommitSize() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);

        TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue<VersionedObject<String>> txnQ1 = context.getQueue("testQueuePollCommitSize");
        TransactionalQueue<VersionedObject<String>> txnQ2 = context.getQueue("testQueuePollCommitSize");

        txnQ1.offer(new VersionedObject<>("item1"));
        txnQ1.offer(new VersionedObject<>("item2"));
        assertEquals(2, txnQ1.size());
        assertEquals(2, txnQ2.size());

        assertEquals(new VersionedObject<>("item1"), txnQ1.poll());
        assertEquals(1, txnQ1.size());
        assertEquals(1, txnQ2.size());
        context.commitTransaction();

        assertEquals(1, instance1.getQueue("testQueuePollCommitSize").size());
        assertEquals(1, instance2.getQueue("testQueuePollCommitSize").size());
        assertEquals(new VersionedObject<>("item2"), instance1.getQueue("testQueuePollCommitSize").poll());
        assertEquals(0, instance1.getQueue("testQueuePollCommitSize").size());
    }

    @Test
    public void testQueuePollRollbackSize() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);

        TransactionContext context = instance1.newTransactionContext();
        IQueue<VersionedObject<String>> queue = instance1.getQueue("testQueuePollRollbackSize");

        queue.offer(new VersionedObject<>("item1", 1));
        queue.offer(new VersionedObject<>("item2", 2));
        assertEquals(2, queue.size());

        context.beginTransaction();
        TransactionalQueue<VersionedObject<String>> txnQ1 = context.getQueue("testQueuePollRollbackSize");

        assertEquals(new VersionedObject<>("item1", 1), txnQ1.poll());
        assertEquals(1, txnQ1.size());
        assertEquals(1, queue.size());

        context.rollbackTransaction();
        assertEquals(2, queue.size());
        assertEquals(new VersionedObject<>("item1", 1), queue.poll());
        assertEquals(new VersionedObject<>("item2", 2), queue.poll());
    }

    @Test
    public void testQueueOrderAfterPollRollback() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);

        TransactionContext context = instance1.newTransactionContext();
        IQueue<VersionedObject<Integer>> queue = instance1.getQueue("testQueueOrderAfterPollRollback");

        context.beginTransaction();
        TransactionalQueue<VersionedObject<Integer>> txn1 = context.getQueue("testQueueOrderAfterPollRollback");
        txn1.offer(new VersionedObject<>(1, 1));
        txn1.offer(new VersionedObject<>(2, 2));
        txn1.offer(new VersionedObject<>(3, 3));
        context.commitTransaction();
        assertEquals(3, queue.size());

        TransactionContext context2 = instance2.newTransactionContext();
        context2.beginTransaction();
        TransactionalQueue<VersionedObject<Integer>> txn2 = context2.getQueue("testQueueOrderAfterPollRollback");
        assertEquals(new VersionedObject<>(1, 1), txn2.poll());
        context2.rollbackTransaction();
        assertEquals(new VersionedObject<>(1, 1), queue.poll());
        assertEquals(new VersionedObject<>(2, 2), queue.poll());
        assertEquals(new VersionedObject<>(3, 3), queue.poll());
    }

    /**
     * Github issue #99
     */
    @Test
    public void issue99TestQueueTakeAndDuringRollback() throws Exception {
        String name = "issue99TestQueueTakeAndDuringRollback";
        HazelcastInstance hz = createHazelcastInstance();

        IQueue<VersionedObject<String>> q = hz.getQueue(name);
        q.offer(new VersionedObject<>("item"));

        Thread t1 = new Thread(() -> {
            TransactionContext context = hz.newTransactionContext();
            try {
                context.beginTransaction();
                context.getQueue(name).poll(1, TimeUnit.DAYS);
                sleep(1000);
                throw new RuntimeException();
            } catch (InterruptedException e) {
                fail(e.getMessage());
            } catch (Exception e) {
                context.rollbackTransaction();
            }
        });
        AtomicBoolean fail = new AtomicBoolean(false);
        Thread t2 = new Thread(() -> {
            TransactionContext context = hz.newTransactionContext();
            try {
                context.beginTransaction();
                context.getQueue(name).poll(1, TimeUnit.DAYS);
                context.commitTransaction();
                fail.set(false);
            } catch (Exception e) {
                context.rollbackTransaction();
                e.printStackTrace();
                fail.set(true);
            }
        });

        t1.start();
        sleep(500);
        t2.start();
        t2.join();
        assertFalse("Queue take failed after rollback!", fail.get());
    }

    /**
     * Github issue #114
     */
    @Test
    public void issue114TestQueueListenersUnderTransaction() throws Exception {
        String name = "issue99TestQueueTakeAndDuringRollback";
        HazelcastInstance hz = createHazelcastInstance();
        IQueue<VersionedObject<String>> testQueue = hz.getQueue(name);

        CountDownLatch offerLatch = new CountDownLatch(2);
        CountDownLatch pollLatch = new CountDownLatch(2);
        testQueue.addItemListener(new ItemListener<VersionedObject<String>>() {
            public void itemAdded(ItemEvent<VersionedObject<String>> item) {
                offerLatch.countDown();
            }

            public void itemRemoved(ItemEvent<VersionedObject<String>> item) {
                pollLatch.countDown();
            }
        }, true);

        TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<VersionedObject<String>> queue = context.getQueue(name);
        queue.offer(new VersionedObject<>("tx Hello"));
        queue.offer(new VersionedObject<>("tx World"));
        context.commitTransaction();

        TransactionContext context2 = hz.newTransactionContext();
        context2.beginTransaction();
        TransactionalQueue<VersionedObject<String>> queue2 = context2.getQueue(name);
        assertEquals(new VersionedObject<>("tx Hello"), queue2.poll());
        assertEquals(new VersionedObject<>("tx World"), queue2.poll());
        context2.commitTransaction();

        assertTrue("Remaining offer listener count: " + offerLatch.getCount(), offerLatch.await(2, TimeUnit.SECONDS));
        assertTrue("Remaining poll listener count: " + pollLatch.getCount(), pollLatch.await(2, TimeUnit.SECONDS));
    }
}
