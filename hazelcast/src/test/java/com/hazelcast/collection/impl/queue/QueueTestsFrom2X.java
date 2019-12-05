/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
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

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueTestsFrom2X extends HazelcastTestSupport {

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void testQueueItemListener() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue("testQueueItemListener");

        final String value = "hello";
        final CountDownLatch latch = new CountDownLatch(8);
        queue.addItemListener(new ItemListener<String>() {
            public void itemAdded(ItemEvent<String> itemEvent) {
                assertEquals(value, itemEvent.getItem());
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<String> itemEvent) {
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
        IQueue<String> queue = instance.getQueue("testQueueAddAll");

        String[] items = new String[]{"one", "two", "three", "four"};

        queue.addAll(asList(items));
        assertEquals(4, queue.size());

        queue.addAll(asList(items));
        assertEquals(8, queue.size());
    }

    @Test
    public void testQueueContains() {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue("testQueueContains");

        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(asList(items));

        assertContains(queue, "one");
        assertContains(queue, "two");
        assertContains(queue, "three");
        assertContains(queue, "four");
    }

    @Test
    public void testQueueContainsAll() {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue("testQueueContainsAll");

        List<String> list = asList("one", "two", "three", "four");
        queue.addAll(list);

        assertContainsAll(queue, list);
    }

    @Test
    public void testQueueRemove() {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> q = instance.getQueue("testQueueRemove");

        for (int i = 0; i < 10; i++) {
            q.offer("item" + i);
        }
        for (int i = 0; i < 5; i++) {
            assertNotNull(q.poll());
        }
        assertEquals("item5", q.peek());

        boolean removed = q.remove("item5");
        assertTrue(removed);

        Iterator<String> it = q.iterator();
        int i = 6;
        while (it.hasNext()) {
            String o = it.next();
            String expectedValue = "item" + (i++);
            assertEquals(o, expectedValue);
        }
        assertEquals(4, q.size());
    }

    @Test
    @Ignore
    public void testInterruption() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(new Config());

        final IQueue<String> queue = instance.getQueue("testInterruption");
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    queue.poll(5, TimeUnit.SECONDS);
                    fail();
                } catch (InterruptedException e) {
                    latch.countDown();
                }
            }
        });

        thread.start();
        sleep(2000);
        thread.interrupt();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        queue.offer("message");
        assertEquals(1, queue.size());
    }

    @Test
    public void issue370() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance h1 = factory.newHazelcastInstance(getConfig());
        final HazelcastInstance h2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(h1, h2);

        final Queue<String> q1 = h1.getQueue("q");
        final Queue<String> q2 = h2.getQueue("q");

        for (int i = 0; i < 5; i++) {
            q1.offer("item" + i);
        }
        assertEquals(5, q1.size());
        assertEquals(5, q2.size());
        assertEquals("item0", q2.poll());
        assertEquals("item1", q2.poll());
        assertEquals("item2", q2.poll());
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());

        h1.shutdown();
        assertEquals(2, q2.size());

        final HazelcastInstance h3 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(h2, h3);
        final Queue<String> q3 = h3.getQueue("q");

        assertEquals(2, q2.size());
        assertEquals(2, q3.size());
        h2.shutdown();
        assertEquals(2, q3.size());
    }

    @Test
    public void issue391() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final int total = 10;
        final Collection<Integer> results = new ArrayList<Integer>(5);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(getConfig());
        final CountDownLatch latchOffer = new CountDownLatch(1);
        final CountDownLatch latchTake = new CountDownLatch(1);

        spawn(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < total; i++) {
                        results.add((Integer) hz1.getQueue("q").take());
                    }
                    latchTake.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        final HazelcastInstance hz2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(hz1, hz2);

        spawn(new Runnable() {
            public void run() {
                for (int i = 0; i < total; i++) {
                    hz2.getQueue("q").offer(i);
                }
                latchOffer.countDown();
            }
        });
        assertTrue(latchOffer.await(100, TimeUnit.SECONDS));
        assertTrue(latchTake.await(10, TimeUnit.SECONDS));
        assertTrue(hz1.getQueue("q").isEmpty());
        hz1.shutdown();
        assertTrue(hz2.getQueue("q").isEmpty());

        final Object[] objects = new Object[total];
        for (int i = 0; i < total; i++) {
            objects[i] = i;
        }
        assertArrayEquals(objects, results.toArray());
    }

    @Test
    public void issue427QOfferIncorrectWithinTransaction() {
        Config config = new Config();
        config.getQueueConfig("default").setMaxSize(100);
        HazelcastInstance instance = createHazelcastInstance(config);
        TransactionContext transactionContext = instance.newTransactionContext();
        transactionContext.beginTransaction();

        TransactionalQueue<Integer> queue = transactionContext.getQueue("default");
        for (int i = 0; i < 100; i++) {
            queue.offer(i);
        }
        boolean result = queue.offer(100);
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
        IQueue<Integer> queue = instance.getQueue(name);

        try {
            final CountDownLatch latch = new CountDownLatch(3);
            ItemListener<Integer> listener = new ItemListener<Integer>() {
                public void itemAdded(ItemEvent<Integer> itemEvent) {
                    latch.countDown();
                }

                public void itemRemoved(ItemEvent<Integer> itemEvent) {
                }
            };

            queue.addItemListener(listener, false);
            queue.offer(1);
            sleep(sleep);
            queue.destroy();
            queue = instance.getQueue(name);
            UUID id = queue.addItemListener(listener, false);
            queue.offer(2);
            sleep(sleep);
            queue.removeItemListener(id);
            queue.offer(3);
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

        TransactionalQueue<String> txnQ1 = context.getQueue("testQueueOfferCommitSize");
        TransactionalQueue<String> txnQ2 = context.getQueue("testQueueOfferCommitSize");
        txnQ1.offer("item");
        assertEquals(1, txnQ1.size());
        assertEquals(1, txnQ2.size());

        context.commitTransaction();
        assertEquals(1, instance1.getQueue("testQueueOfferCommitSize").size());
        assertEquals(1, instance2.getQueue("testQueueOfferCommitSize").size());
        assertEquals("item", instance2.getQueue("testQueueOfferCommitSize").poll());
    }

    @Test
    public void testQueueOfferRollbackSize() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);

        TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue<String> txnQ1 = context.getQueue("testQueueOfferRollbackSize");
        TransactionalQueue<String> txnQ2 = context.getQueue("testQueueOfferRollbackSize");

        txnQ1.offer("item");
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

        TransactionalQueue<String> txnQ1 = context.getQueue("testQueuePollCommitSize");
        TransactionalQueue<String> txnQ2 = context.getQueue("testQueuePollCommitSize");

        txnQ1.offer("item1");
        txnQ1.offer("item2");
        assertEquals(2, txnQ1.size());
        assertEquals(2, txnQ2.size());

        assertEquals("item1", txnQ1.poll());
        assertEquals(1, txnQ1.size());
        assertEquals(1, txnQ2.size());
        context.commitTransaction();

        assertEquals(1, instance1.getQueue("testQueuePollCommitSize").size());
        assertEquals(1, instance2.getQueue("testQueuePollCommitSize").size());
        assertEquals("item2", instance1.getQueue("testQueuePollCommitSize").poll());
        assertEquals(0, instance1.getQueue("testQueuePollCommitSize").size());
    }

    @Test
    public void testQueuePollRollbackSize() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        final HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);

        TransactionContext context = instance1.newTransactionContext();
        IQueue<Object> queue = instance1.getQueue("testQueuePollRollbackSize");

        queue.offer("item1");
        queue.offer("item2");
        assertEquals(2, queue.size());

        context.beginTransaction();
        TransactionalQueue txnQ1 = context.getQueue("testQueuePollRollbackSize");

        assertEquals("item1", txnQ1.poll());
        assertEquals(1, txnQ1.size());
        assertEquals(1, queue.size());

        context.rollbackTransaction();
        assertEquals(2, queue.size());
        assertEquals("item1", queue.poll());
        assertEquals("item2", queue.poll());
    }

    @Test
    public void testQueueOrderAfterPollRollback() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);

        TransactionContext context = instance1.newTransactionContext();
        IQueue<Integer> queue = instance1.getQueue("testQueueOrderAfterPollRollback");

        context.beginTransaction();
        TransactionalQueue<Integer> txn1 = context.getQueue("testQueueOrderAfterPollRollback");
        txn1.offer(1);
        txn1.offer(2);
        txn1.offer(3);
        context.commitTransaction();
        assertEquals(3, queue.size());

        TransactionContext context2 = instance2.newTransactionContext();
        context2.beginTransaction();
        TransactionalQueue<Integer> txn2 = context2.getQueue("testQueueOrderAfterPollRollback");
        assertEquals(1, txn2.poll().intValue());
        context2.rollbackTransaction();
        assertEquals(1, queue.poll().intValue());
        assertEquals(2, queue.poll().intValue());
        assertEquals(3, queue.poll().intValue());
    }

    /**
     * Github issue #99
     */
    @Test
    public void issue99TestQueueTakeAndDuringRollback() throws Exception {
        final String name = "issue99TestQueueTakeAndDuringRollback";
        final HazelcastInstance hz = createHazelcastInstance();

        IQueue<String> q = hz.getQueue(name);
        q.offer("item");

        Thread t1 = new Thread() {
            public void run() {
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
            }
        };
        final AtomicBoolean fail = new AtomicBoolean(false);
        Thread t2 = new Thread() {
            public void run() {
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
            }
        };

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
        IQueue<String> testQueue = hz.getQueue(name);

        final CountDownLatch offerLatch = new CountDownLatch(2);
        final CountDownLatch pollLatch = new CountDownLatch(2);
        testQueue.addItemListener(new ItemListener<String>() {
            public void itemAdded(ItemEvent<String> item) {
                offerLatch.countDown();
            }

            public void itemRemoved(ItemEvent<String> item) {
                pollLatch.countDown();
            }
        }, true);

        TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<Object> queue = context.getQueue(name);
        queue.offer("tx Hello");
        queue.offer("tx World");
        context.commitTransaction();

        TransactionContext context2 = hz.newTransactionContext();
        context2.beginTransaction();
        TransactionalQueue<Object> queue2 = context2.getQueue(name);
        assertEquals("tx Hello", queue2.poll());
        assertEquals("tx World", queue2.poll());
        context2.commitTransaction();

        assertTrue("Remaining offer listener count: " + offerLatch.getCount(), offerLatch.await(2, TimeUnit.SECONDS));
        assertTrue("Remaining poll listener count: " + pollLatch.getCount(), pollLatch.await(2, TimeUnit.SECONDS));
    }
}
