package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * @ali 6/3/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class QueueTestsFrom2X extends HazelcastTestSupport {

    @Test
    public void testQueueItemListener() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(8);
        final String value = "hello";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(null);
        IQueue<String> queue = instance.getQueue("testQueueItemListener");
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
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(null);
        IQueue<String> queue = instance.getQueue("testQueueAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertEquals(4, queue.size());
        queue.addAll(Arrays.asList(items));
        assertEquals(8, queue.size());
    }

    @Test
    public void testQueueContains() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(null);
        IQueue<String> queue = instance.getQueue("testQueueContains");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertTrue(queue.contains("one"));
        assertTrue(queue.contains("two"));
        assertTrue(queue.contains("three"));
        assertTrue(queue.contains("four"));
    }

    @Test
    public void testQueueContainsAll() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(null);
        IQueue<String> queue = instance.getQueue("testQueueContainsAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        List<String> list = Arrays.asList(items);
        queue.addAll(list);
        assertTrue(queue.containsAll(list));
    }

    @Test
    public void testQueueRemove() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(new Config());
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

//    @Test
//    public void testInterruption() throws InterruptedException {
//        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
//        final HazelcastInstance instance = factory.newHazelcastInstance(new Config());
//        final IQueue<String> q = instance.getQueue("testInterruption");
//        final CountDownLatch latch = new CountDownLatch(1);
//        Thread t = new Thread(new Runnable() {
//            public void run() {
//                try {
//                    q.poll(5, TimeUnit.SECONDS);
//                    fail();
//                } catch (InterruptedException e) {
//                    latch.countDown();
//                }
//            }
//        });
//        t.start();
//        Thread.sleep(2000);
//        t.interrupt();
//        assertTrue(latch.await(100, TimeUnit.SECONDS));
//        q.offer("message");
//        assertEquals(1, q.size());
//    }

    @Test
    public void issue370() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance h2 = factory.newHazelcastInstance(new Config());
        Queue<String> q1 = h1.getQueue("q");
        Queue<String> q2 = h2.getQueue("q");
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

        h1.getLifecycleService().shutdown();
        assertEquals(2, q2.size());
        h1 = factory.newHazelcastInstance(new Config());
        q1 = h1.getQueue("q");
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());
        h2.getLifecycleService().shutdown();
        assertEquals(2, q1.size());
    }

    @Test
    public void issue391() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        final Collection<String> results = new ArrayList<String>(5);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        final CountDownLatch latchOffer = new CountDownLatch(1);
        final CountDownLatch latchTake = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < 5; i++) {
                        results.add((String) hz1.getQueue("q").take());
                    }
                    latchTake.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        final HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 5; i++) {
                    boolean offered = hz2.getQueue("q").offer(Integer.toString(i));
                    System.err.println("offered: " + offered);
                }
                System.err.println("done");
                latchOffer.countDown();
            }
        }).start();
        Assert.assertTrue(latchOffer.await(100, TimeUnit.SECONDS));
        Assert.assertTrue(latchTake.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(hz1.getQueue("q").isEmpty());
        hz1.getLifecycleService().shutdown();
        Assert.assertTrue(hz2.getQueue("q").isEmpty());
        assertArrayEquals(new Object[]{"0", "1", "2", "3", "4"}, results.toArray());
    }

    @Test
    public void issue427QOfferIncorrectWithinTransaction() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);

        Config config = new Config();
        config.getQueueConfig("default").setMaxSize(100);
        HazelcastInstance h = factory.newHazelcastInstance(config);
        final TransactionContext transactionContext = h.newTransactionContext();
        transactionContext.beginTransaction();
        final TransactionalQueue q = transactionContext.getQueue("default");
        for (int i = 0; i < 100; i++) {
            q.offer(i);
        }
        boolean result = q.offer(100);
        assertEquals(100, q.size());
        transactionContext.commitTransaction();

        assertEquals(100, h.getQueue("default").size());
        assertFalse(result);
        h.getLifecycleService().shutdown();
    }

    @Test
    public void testListenerLifecycle() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(new Config());
        // IQueue
        final long sleep = 2000;
        final String name = "listenerLifecycle";
        IQueue q = instance.getQueue(name);
        final CountDownLatch qLatch = new CountDownLatch(3) ;
        final ItemListener ql = new ItemListener() {
            public void itemAdded(ItemEvent itemEvent) {
                qLatch.countDown();
            }

            public void itemRemoved(ItemEvent itemEvent) {
            }
        };
        q.addItemListener(ql, false);
        q.offer(1);
        Thread.sleep(sleep);
        q.destroy();
        q = instance.getQueue(name);
        String id = q.addItemListener(ql, false);
        q.offer(2);
        Thread.sleep(sleep);
        q.removeItemListener(id);
        q.offer(3);
        Thread.sleep(sleep);
        assertEquals(1, qLatch.getCount());
        qLatch.countDown();
        assertTrue("Remaining:" + qLatch.getCount(), qLatch.await(3, TimeUnit.SECONDS));
        q.destroy();
    }

    @Test
    public void testQueueOfferCommitSize() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(new Config());
        final HazelcastInstance instance2 = factory.newHazelcastInstance(new Config());
        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue txnQ1 = context.getQueue("testQueueOfferCommitSize");
        TransactionalQueue txnQ2 = context.getQueue("testQueueOfferCommitSize");
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
        final HazelcastInstance instance1 = factory.newHazelcastInstance(new Config());
        final HazelcastInstance instance2 = factory.newHazelcastInstance(new Config());
        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue txnQ1 = context.getQueue("testQueueOfferRollbackSize");
        TransactionalQueue txnQ2 = context.getQueue("testQueueOfferRollbackSize");

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
        final HazelcastInstance instance1 = factory.newHazelcastInstance(new Config());
        final HazelcastInstance instance2 = factory.newHazelcastInstance(new Config());
        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        TransactionalQueue txnQ1 = context.getQueue("testQueuePollCommitSize");
        TransactionalQueue txnQ2 = context.getQueue("testQueuePollCommitSize");

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
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(new Config());
        final HazelcastInstance instance2 = factory.newHazelcastInstance(new Config());
        final TransactionContext context = instance1.newTransactionContext();
        final IQueue<Object> q = instance1.getQueue("testQueuePollRollbackSize");

        q.offer("item1");
        q.offer("item2");
        assertEquals(2, q.size());

        context.beginTransaction();
        TransactionalQueue txnQ1 = context.getQueue("testQueuePollRollbackSize");

        assertEquals("item1", txnQ1.poll());
        assertEquals(1, txnQ1.size());
        assertEquals(1, q.size());
        context.rollbackTransaction();
        assertEquals(2, q.size());
        assertEquals("item1", q.poll());
        assertEquals("item2", q.poll());
    }

    @Test
    public void testQueueOrderAfterPollRollback() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(new Config());
        final HazelcastInstance instance2 = factory.newHazelcastInstance(new Config());
        final TransactionContext context = instance1.newTransactionContext();
        final IQueue<Integer> queue = instance1.getQueue("testQueueOrderAfterPollRollback");

        context.beginTransaction();
        TransactionalQueue<Integer> txn1 = context.getQueue("testQueueOrderAfterPollRollback");
        txn1.offer(1);
        txn1.offer(2);
        txn1.offer(3);
        context.commitTransaction();

        assertEquals(3, queue.size());

        final TransactionContext context2 = instance2.newTransactionContext();
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
    public void issue99TestQueueTakeAndDuringRollback() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        final String name = "issue99TestQueueTakeAndDuringRollback";
        final IQueue q = hz.getQueue(name);
        q.offer("item");

        Thread t1 = new Thread() {
            public void run() {
                final TransactionContext context = hz.newTransactionContext();
                try {
                    context.beginTransaction();
                    final Object polled = context.getQueue(name).poll(1, TimeUnit.DAYS);
                    sleep(1000);
                    System.err.println("polled " + polled + " throwing now");
                    throw new RuntimeException();
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                } catch (Exception e) {
                    System.err.println("rollback");
                    context.rollbackTransaction();
                }
            }
        };
        final AtomicBoolean fail = new AtomicBoolean(false);
        Thread t2 = new Thread() {
            public void run() {
                final TransactionContext context = hz.newTransactionContext();
                try {
                    context.beginTransaction();
                    System.err.println("polling");
                    context.getQueue(name).poll(1, TimeUnit.DAYS);
                    System.err.println("polled");
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
        Thread.sleep(500);
        t2.start();
        t2.join();
        assertFalse("Queue take failed after rollback!", fail.get());
    }

    /**
     * Github issue #114
     */
    @Test
    public void issue114TestQueueListenersUnderTransaction() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        final String name = "issue99TestQueueTakeAndDuringRollback";
        final IQueue testQueue = hz.getQueue(name);

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

        final TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        final TransactionalQueue<Object> queue = context.getQueue(name);
        queue.offer("tx Hello");
        queue.offer("tx World");
        context.commitTransaction();

        final TransactionContext context2 = hz.newTransactionContext();
        context2.beginTransaction();
        final TransactionalQueue<Object> queue2 = context2.getQueue(name);
        Assert.assertEquals("tx Hello", queue2.poll());
        Assert.assertEquals("tx World", queue2.poll());
        context2.commitTransaction();

        Assert.assertTrue("Remaining offer listener count: " + offerLatch.getCount(), offerLatch.await(2, TimeUnit.SECONDS));
        Assert.assertTrue("Remaining poll listener count: " + pollLatch.getCount(), pollLatch.await(2, TimeUnit.SECONDS));
    }


}
