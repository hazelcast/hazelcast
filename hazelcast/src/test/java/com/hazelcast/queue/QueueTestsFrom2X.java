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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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

        final Collection<String> results = new CopyOnWriteArrayList<String>();
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
                    hz2.getQueue("q").offer(Integer.toString(i));
                }
                latchOffer.countDown();
            }
        }).start();
        Assert.assertTrue(latchOffer.await(10, TimeUnit.SECONDS));
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
}
