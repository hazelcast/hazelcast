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
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.collection.IQueue;
import com.hazelcast.cluster.MemberAttributeEvent;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueAdvancedTest extends HazelcastTestSupport {

    @Test
    public void testOffer() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        IQueue<String> q1 = h1.getQueue("default");
        IQueue<String> q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertTrue("Expected q1.offer() to succeed", q1.offer("item" + i, 100, SECONDS));
            assertTrue("Expected q2.offer() to succeed", q2.offer("item" + i, 100, SECONDS));
        }
        assertEquals("item0", q1.peek());
        assertEquals("item0", q2.peek());
        for (int i = 0; i < 100; i++) {
            assertEquals("item" + i, q1.poll());
            assertEquals("item" + i, q2.poll());
        }
    }

    /**
     * Test for issue 730 (Google).
     */
    @Test
    public void testDeadTaker() throws Exception {
        Config config = new Config();
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        config.addListenerConfig(new ListenerConfig().setImplementation(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                shutdownLatch.countDown();
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            }
        }));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
        warmUpPartitions(h1, h2);

        final IQueue<String> q1 = h1.getQueue("default");
        final IQueue<String> q2 = h2.getQueue("default");

        final CountDownLatch startLatch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    assertTrue("Expected startLatch.await() to succeed within 10 seconds", startLatch.await(10, SECONDS));
                    Thread.sleep(5000);
                    h2.getLifecycleService().terminate();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startLatch.countDown();
                    String value = q2.take();
                    fail("Should not be able to take value from queue, but got: " + value);
                } catch (HazelcastInstanceNotActiveException e) {
                    ignore(e);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        assertTrue("Expected shutdownLatch.await() to succeed within 1 minute", shutdownLatch.await(1, MINUTES));

        q1.offer("item");
        assertEquals(1, q1.size());
        assertEquals("item", q1.poll());
    }

    @Test
    public void testShutdown() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        warmUpPartitions(h2, h1);

        IQueue<String> q1 = h1.getQueue("default");
        IQueue<String> q2 = h2.getQueue("default");
        for (int i = 0; i < 40; i++) {
            assertTrue("Expected q1.offer() to succeed", q1.offer("item" + i, 100, SECONDS));
        }
        h1.getLifecycleService().shutdown();
        for (int i = 40; i < 100; i++) {
            assertTrue("Expected q2.offer() to succeed", q2.offer("item" + i, 100, SECONDS));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("item" + i, q2.poll());
        }
    }

    @Test
    public void testPollNull() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue q1 = h1.getQueue("default");
        IQueue q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertNull(q1.poll());
            assertNull(q2.poll());
        }
        assertNull(q1.poll(2, SECONDS));
        assertNull(q2.poll(2, SECONDS));
    }

    @Test
    public void testTake() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        final IQueue<String> q1 = h1.getQueue("default");
        final IQueue<String> q2 = h2.getQueue("default");

        final CountDownLatch offerLatch = new CountDownLatch(2 * 100);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    for (int i = 0; i < 100; i++) {
                        if (q1.offer("item")) {
                            offerLatch.countDown();
                        }
                        if (q2.offer("item")) {
                            offerLatch.countDown();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        assertOpenEventually(offerLatch);

        ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if ("item".equals(q1.take())) {
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if ("item".equals(q2.take())) {
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        assertOpenEventually(latch);
        es.shutdown();
    }

    @Test
    public void testPollLong() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        final IQueue<String> q1 = h1.getQueue("default");
        final IQueue<String> q2 = h2.getQueue("default");

        final CountDownLatch offerLatch = new CountDownLatch(2 * 100);
        Thread.sleep(1000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    if (q1.offer("item")) {
                        offerLatch.countDown();
                    }
                    if (q2.offer("item")) {
                        offerLatch.countDown();
                    }
                }
            }
        }).start();
        assertOpenEventually(offerLatch);

        ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        Thread.sleep(3000);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if ("item".equals(q1.poll(5, SECONDS))) {
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if ("item".equals(q2.poll(5, SECONDS))) {
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        assertOpenEventually(latch);
        es.shutdown();
    }

    @Test
    public void testOfferLong() throws Exception {
        Config config = new Config();
        config.getQueueConfig("default").setMaxSize(200);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        final IQueue<String> q1 = h1.getQueue("default");
        final IQueue<String> q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertTrue("Expected q1.offer() to succeed", q1.offer("item" + i, 100, SECONDS));
            assertTrue("Expected q2.offer() to succeed", q2.offer("item" + i, 100, SECONDS));
        }
        assertFalse(q1.offer("item"));
        assertFalse(q2.offer("item"));
        assertFalse(q1.offer("item", 2, SECONDS));
        assertFalse(q2.offer("item", 2, SECONDS));

        final CountDownLatch pollLatch = new CountDownLatch(200);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    for (int i = 0; i < 100; i++) {
                        if (("item" + i).equals(q1.poll(2, SECONDS))) {
                            pollLatch.countDown();
                        }
                        if (("item" + i).equals(q2.poll(2, SECONDS))) {
                            pollLatch.countDown();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        assertOpenEventually(pollLatch);

        ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (q1.offer("item", 30, SECONDS)) {
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (q2.offer("item", 30, SECONDS)) {
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        assertOpenEventually(latch);
        es.shutdown();
    }

    /**
     * Test case for issue 289.
     * <p>
     * 1. Create HazelcastInstance h1 and h2, then get a queue from each (same queue name)
     * 2. Put a message on queue from h2
     * 3. Take a message off on queue from h1
     * 4. Shutdown h1, then check if queue is still empty on h2
     */
    @Test
    public void testQueueAfterShutdown() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue<String> q1 = h1.getQueue("default");
        IQueue<String> q2 = h2.getQueue("default");

        q2.offer("item");
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());

        assertEquals("item", q1.take());
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());

        h1.getLifecycleService().shutdown();
        assertEquals(0, q2.size());
    }

    @Test
    public void testQueueAfterShutdown_switchedInstanceOrder() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue<String> q1 = h1.getQueue("default");
        IQueue<String> q2 = h2.getQueue("default");

        q1.offer("item");
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());

        assertEquals("item", q2.take());
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());

        h2.getLifecycleService().shutdown();
        assertEquals(0, q1.size());
    }

    @Test
    public void queueEntriesShouldBeConsistentAfterShutdown() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

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
        assertSizeEventually(2, q1);
        assertSizeEventually(2, q2);

        h1.getLifecycleService().shutdown();
        assertSizeEventually(2, q2);
    }

    @Test
    public void queueEntriesShouldBeConsistentAfterShutdown_switchedInstanceOrder() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        Queue<String> q1 = h1.getQueue("q");
        Queue<String> q2 = h2.getQueue("q");

        for (int i = 0; i < 5; i++) {
            q2.offer("item" + i);
        }
        assertEquals(5, q1.size());
        assertEquals(5, q2.size());

        assertEquals("item0", q1.poll());
        assertEquals("item1", q1.poll());
        assertEquals("item2", q1.poll());
        assertSizeEventually(2, q1);
        assertSizeEventually(2, q2);

        h2.getLifecycleService().shutdown();
        assertSizeEventually(2, q1);
    }

    @Test
    public void testAddAllBackup() {
        HazelcastInstance[] instances = createHazelcastInstances();
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        String name = generateKeyOwnedBy(instance1);

        IQueue<String> queue1 = instance1.getQueue(name);
        IQueue<String> queue2 = instance2.getQueue(name);

        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 4; i++) {
            list.add("item" + i);
        }
        assertTrue("Expected queue1.addAll() to succeed", queue1.addAll(list));

        instance1.shutdown();

        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2, "item0", "item1", "item2", "item3");
    }

    @Test
    public void testClearBackup() {
        HazelcastInstance[] instances = createHazelcastInstances();
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        String name = generateKeyOwnedBy(instance1);

        IQueue<String> queue1 = instance1.getQueue(name);
        IQueue<String> queue2 = instance2.getQueue(name);

        for (int i = 0; i < 4; i++) {
            queue1.offer("item" + i);
        }
        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2, "item0", "item1", "item2", "item3");
        queue1.clear();

        instance1.shutdown();

        assertSizeEventually(0, queue2);
    }

    @Test
    public void testRemoveBackup() {
        HazelcastInstance[] instances = createHazelcastInstances();
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        String name = generateKeyOwnedBy(instance1);

        IQueue<String> queue1 = instance1.getQueue(name);
        IQueue<String> queue2 = instance2.getQueue(name);

        for (int i = 0; i < 4; i++) {
            queue1.offer("item" + i);
        }

        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2, "item0", "item1", "item2", "item3");
        queue1.remove("item0");
        queue1.remove("item1");

        instance1.shutdown();

        assertSizeEventually(2, queue2);
        assertIterableEquals(queue2, "item2", "item3");
    }

    @Test
    public void testCompareAndRemoveBackup() {
        HazelcastInstance[] instances = createHazelcastInstances();
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        String name = generateKeyOwnedBy(instance1);

        IQueue<String> queue1 = instance1.getQueue(name);
        IQueue<String> queue2 = instance2.getQueue(name);

        for (int i = 0; i < 4; i++) {
            queue1.offer("item" + i);
        }
        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2, "item0", "item1", "item2", "item3");

        List<String> list = new ArrayList<String>();
        list.add("item0");
        list.add("item1");
        list.add("item2");

        assertTrue("Expected queue1.removeAll() to succeed", queue1.removeAll(list));
        instance1.shutdown();
        assertSizeEventually(1, queue2);
        assertIterableEquals(queue2, "item3");
    }

    @Test
    public void testDrainBackup() {
        HazelcastInstance[] instances = createHazelcastInstances();
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        String name = generateKeyOwnedBy(instance1);

        IQueue<String> queue1 = instance1.getQueue(name);
        IQueue<String> queue2 = instance2.getQueue(name);

        for (int i = 0; i < 4; i++) {
            queue1.offer("item" + i);
        }

        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2, "item0", "item1", "item2", "item3");

        List<String> list = new ArrayList<String>();
        queue1.drainTo(list, 2);

        instance1.shutdown();

        assertSizeEventually(2, queue2);
        assertIterableEquals(queue2, "item2", "item3");
    }

    @Test
    public void testTakeInterruption() {
        Config config = new Config()
                .setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "10000");

        HazelcastInstance instance = createHazelcastInstance(config);
        final IQueue<Thread> queue = instance.getQueue(randomName());

        final CountDownLatch takeLatch = new CountDownLatch(1);
        TestThread thread = new TestThread() {
            @Override
            public void doRun() throws Throwable {
                takeLatch.countDown();
                queue.take();
            }
        };
        thread.start();

        assertOpenEventually(takeLatch);

        thread.interrupt();

        thread.assertFailsEventually(InterruptedException.class);
    }

    @Test
    public void testPutInterruption() {
        Config config = new Config()
                .setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "10000");
        config.getQueueConfig("default").setMaxSize(1);

        HazelcastInstance instance = createHazelcastInstance(config);
        final IQueue<String> queue = instance.getQueue(randomName());

        assertTrue("Expected queue.offer() to succeed", queue.offer("item"));

        final CountDownLatch putLatch = new CountDownLatch(1);
        TestThread thread = new TestThread() {
            @Override
            public void doRun() throws Throwable {
                putLatch.countDown();
                queue.put("item");
            }
        };
        thread.start();

        assertOpenEventually(putLatch);

        thread.interrupt();

        thread.assertFailsEventually(InterruptedException.class);
    }

    private HazelcastInstance[] createHazelcastInstances() {
        String configName = randomString();
        Config config = new Config();
        config.getQueueConfig(configName).setMaxSize(100);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        return factory.newInstances(config);
    }
}
