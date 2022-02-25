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

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueAdvancedTest extends HazelcastTestSupport {

    private static final ILogger LOG = Logger.getLogger(QueueAdvancedTest.class);

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Test
    public void testOffer() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertTrue("Expected q1.offer() to succeed", q1.offer(new VersionedObject<>("item" + i, i), 100, SECONDS));
            assertTrue("Expected q2.offer() to succeed", q2.offer(new VersionedObject<>("item" + i, i), 100, SECONDS));
        }
        assertEquals(new VersionedObject<>("item0", 0), q1.peek());
        assertEquals(new VersionedObject<>("item0", 0), q2.peek());
        for (int i = 0; i < 100; i++) {
            assertEquals(new VersionedObject<>("item" + i, i), q1.poll());
            assertEquals(new VersionedObject<>("item" + i, i), q2.poll());
        }
    }

    /**
     * Test for issue 730 (Google).
     */
    @Test
    public void testDeadTaker() throws Exception {
        Config config = getConfig();
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        config.addListenerConfig(new ListenerConfig().setImplementation(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                shutdownLatch.countDown();
            }

        }));

        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(config);
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        warmUpPartitions(h1, h2);

        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");

        CountDownLatch startLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                assertTrue("Expected startLatch.await() to succeed within 10 seconds", startLatch.await(10, SECONDS));
                Thread.sleep(5000);
                h2.getLifecycleService().terminate();
            } catch (InterruptedException e) {
                LOG.info(e);
            }
        }).start();

        new Thread(() -> {
            try {
                startLatch.countDown();
                VersionedObject<String> value = q2.take();
                fail("Should not be able to take value from queue, but got: " + value);
            } catch (HazelcastInstanceNotActiveException e) {
                ignore(e);
            } catch (InterruptedException e) {
                LOG.info(e);
            }
        }).start();

        assertTrue("Expected shutdownLatch.await() to succeed within 1 minute", shutdownLatch.await(1, MINUTES));

        q1.offer(new VersionedObject<>("item"));
        assertEquals(1, q1.size());
        assertEquals(new VersionedObject<>("item"), q1.poll());
    }

    @Test
    public void testShutdown() throws InterruptedException {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        warmUpPartitions(h2, h1);

        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");
        for (int i = 0; i < 40; i++) {
            assertTrue("Expected q1.offer() to succeed", q1.offer(new VersionedObject<>("item" + i, i), 100, SECONDS));
        }
        h1.getLifecycleService().shutdown();
        for (int i = 40; i < 100; i++) {
            assertTrue("Expected q2.offer() to succeed", q2.offer(new VersionedObject<>("item" + i, i), 100, SECONDS));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(new VersionedObject<>("item" + i, i), q2.poll());
        }
    }

    @Test
    public void testPollNull() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertNull(q1.poll());
            assertNull(q2.poll());
        }
        assertNull(q1.poll(2, SECONDS));
        assertNull(q2.poll(2, SECONDS));
    }

    @Test
    public void testTake() {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");

        CountDownLatch offerLatch = new CountDownLatch(2 * 100);
        new Thread(() -> {
            try {
                Thread.sleep(3000);
                for (int i = 0; i < 100; i++) {
                    if (q1.offer(new VersionedObject<>("item"))) {
                        offerLatch.countDown();
                    }
                    if (q2.offer(new VersionedObject<>("item"))) {
                        offerLatch.countDown();
                    }
                }
            } catch (InterruptedException e) {
                LOG.info(e);
            }
        }).start();

        assertOpenEventually(offerLatch);

        ExecutorService es = Executors.newFixedThreadPool(50);
        CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(() -> {
                try {
                    if (new VersionedObject<>("item").equals(q1.take())) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    LOG.info(e);
                }
            });
            es.execute(() -> {
                try {
                    if (new VersionedObject<>("item").equals(q2.take())) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    LOG.info(e);
                }
            });
        }

        assertOpenEventually(latch);
        es.shutdown();
    }

    @Test
    public void testPollLong() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");

        CountDownLatch offerLatch = new CountDownLatch(2 * 100);
        Thread.sleep(1000);
        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                if (q1.offer(new VersionedObject<>("item"))) {
                    offerLatch.countDown();
                }
                if (q2.offer(new VersionedObject<>("item"))) {
                    offerLatch.countDown();
                }
            }
        }).start();
        assertOpenEventually(offerLatch);

        ExecutorService es = Executors.newFixedThreadPool(50);
        CountDownLatch latch = new CountDownLatch(200);
        Thread.sleep(3000);
        for (int i = 0; i < 100; i++) {
            es.execute(() -> {
                try {
                    if (new VersionedObject<>("item").equals(q1.poll(5, SECONDS))) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    LOG.info(e);
                }
            });
            es.execute(() -> {
                try {
                    if (new VersionedObject<>("item").equals(q2.poll(5, SECONDS))) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    LOG.info(e);
                }
            });
        }
        assertOpenEventually(latch);
        es.shutdown();
    }

    @Test
    public void testOfferLong() throws InterruptedException {
        Config config = getConfig();
        config.getQueueConfig("default").setMaxSize(200);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertTrue("Expected q1.offer() to succeed", q1.offer(new VersionedObject<>("item" + i, i), 100, SECONDS));
            assertTrue("Expected q2.offer() to succeed", q2.offer(new VersionedObject<>("item" + i, i), 100, SECONDS));
        }
        assertFalse(q1.offer(new VersionedObject<>("item")));
        assertFalse(q2.offer(new VersionedObject<>("item")));
        assertFalse(q1.offer(new VersionedObject<>("item"), 2, SECONDS));
        assertFalse(q2.offer(new VersionedObject<>("item"), 2, SECONDS));

        CountDownLatch pollLatch = new CountDownLatch(200);
        new Thread(() -> {
            try {
                Thread.sleep(3000);
                for (int i = 0; i < 100; i++) {
                    if (new VersionedObject<>("item" + i, i).equals(q1.poll(2, SECONDS))) {
                        pollLatch.countDown();
                    }
                    if (new VersionedObject<>("item" + i, i).equals(q2.poll(2, SECONDS))) {
                        pollLatch.countDown();
                    }
                }
            } catch (InterruptedException e) {
                LOG.info(e);
            }
        }).start();
        assertOpenEventually(pollLatch);

        ExecutorService es = Executors.newFixedThreadPool(50);
        CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(() -> {
                try {
                    if (q1.offer(new VersionedObject<>("item"), 30, SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    LOG.info(e);
                }
            });
            es.execute(() -> {
                try {
                    if (q2.offer(new VersionedObject<>("item"), 30, SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    LOG.info(e);
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
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");

        q2.offer(new VersionedObject<>("item"));
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());

        assertEquals(new VersionedObject<>("item"), q1.take());
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());

        h1.getLifecycleService().shutdown();
        assertEquals(0, q2.size());
    }

    @Test
    public void testQueueAfterShutdown_switchedInstanceOrder() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        IQueue<VersionedObject<String>> q1 = h1.getQueue("default");
        IQueue<VersionedObject<String>> q2 = h2.getQueue("default");

        q1.offer(new VersionedObject<>("item"));
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());

        assertEquals(new VersionedObject<>("item"), q2.take());
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());

        h2.getLifecycleService().shutdown();
        assertEquals(0, q1.size());
    }

    @Test
    public void queueEntriesShouldBeConsistentAfterShutdown() {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

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
        assertSizeEventually(2, q1);
        assertSizeEventually(2, q2);

        h1.getLifecycleService().shutdown();
        assertSizeEventually(2, q2);
    }

    @Test
    public void queueEntriesShouldBeConsistentAfterShutdown_switchedInstanceOrder() {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];

        Queue<VersionedObject<String>> q1 = h1.getQueue("q");
        Queue<VersionedObject<String>> q2 = h2.getQueue("q");

        for (int i = 0; i < 5; i++) {
            q2.offer(new VersionedObject<>("item" + i, i));
        }
        assertEquals(5, q1.size());
        assertEquals(5, q2.size());

        assertEquals(new VersionedObject<>("item0", 0), q1.poll());
        assertEquals(new VersionedObject<>("item1", 1), q1.poll());
        assertEquals(new VersionedObject<>("item2", 2), q1.poll());
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

        IQueue<VersionedObject<String>> queue1 = instance1.getQueue(name);
        IQueue<VersionedObject<String>> queue2 = instance2.getQueue(name);

        List<VersionedObject<String>> list = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            list.add(new VersionedObject<>("item" + i, i));
        }
        assertTrue("Expected queue1.addAll() to succeed", queue1.addAll(list));

        instance1.shutdown();

        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2,
                new VersionedObject<>("item0", 0),
                new VersionedObject<>("item1", 1),
                new VersionedObject<>("item2", 2),
                new VersionedObject<>("item3", 3));
    }

    @Test
    public void testClearBackup() {
        HazelcastInstance[] instances = createHazelcastInstances();
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        String name = generateKeyOwnedBy(instance1);

        IQueue<VersionedObject<String>> queue1 = instance1.getQueue(name);
        IQueue<VersionedObject<String>> queue2 = instance2.getQueue(name);

        for (int i = 0; i < 4; i++) {
            queue1.offer(new VersionedObject<>("item" + i, i));
        }
        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2,
                new VersionedObject<>("item0", 0),
                new VersionedObject<>("item1", 1),
                new VersionedObject<>("item2", 2),
                new VersionedObject<>("item3", 3));
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

        IQueue<VersionedObject<String>> queue1 = instance1.getQueue(name);
        IQueue<VersionedObject<String>> queue2 = instance2.getQueue(name);

        for (int i = 0; i < 4; i++) {
            queue1.offer(new VersionedObject<>("item" + i, i));
        }

        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2,
                new VersionedObject<>("item0", 0),
                new VersionedObject<>("item1", 1),
                new VersionedObject<>("item2", 2),
                new VersionedObject<>("item3", 3));
        queue1.remove(new VersionedObject<>("item0", 0));
        queue1.remove(new VersionedObject<>("item1", 1));

        instance1.shutdown();

        assertSizeEventually(2, queue2);
        assertIterableEquals(queue2, new VersionedObject<>("item2", 2), new VersionedObject<>("item3", 3));
    }

    @Test
    public void testCompareAndRemoveBackup() {
        HazelcastInstance[] instances = createHazelcastInstances();
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        String name = generateKeyOwnedBy(instance1);

        IQueue<VersionedObject<String>> queue1 = instance1.getQueue(name);
        IQueue<VersionedObject<String>> queue2 = instance2.getQueue(name);

        for (int i = 0; i < 4; i++) {
            queue1.offer(new VersionedObject<>("item" + i, i));
        }
        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2,
                new VersionedObject<>("item0", 0),
                new VersionedObject<>("item1", 1),
                new VersionedObject<>("item2", 2),
                new VersionedObject<>("item3", 3));

        List<VersionedObject<String>> list = new ArrayList<>();
        list.add(new VersionedObject<>("item0", 0));
        list.add(new VersionedObject<>("item1", 1));
        list.add(new VersionedObject<>("item2", 2));

        assertTrue("Expected queue1.removeAll() to succeed", queue1.removeAll(list));
        instance1.shutdown();
        assertSizeEventually(1, queue2);
        assertIterableEquals(queue2, new VersionedObject<>("item3", 3));
    }

    @Test
    public void testDrainBackup() {
        HazelcastInstance[] instances = createHazelcastInstances();
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        String name = generateKeyOwnedBy(instance1);

        IQueue<VersionedObject<String>> queue1 = instance1.getQueue(name);
        IQueue<VersionedObject<String>> queue2 = instance2.getQueue(name);

        for (int i = 0; i < 4; i++) {
            queue1.offer(new VersionedObject<>("item" + i, i));
        }

        assertSizeEventually(4, queue2);
        assertIterableEquals(queue2,
                new VersionedObject<>("item0", 0),
                new VersionedObject<>("item1", 1),
                new VersionedObject<>("item2", 2),
                new VersionedObject<>("item3", 3));

        List<VersionedObject<String>> list = new ArrayList<>();
        queue1.drainTo(list, 2);

        instance1.shutdown();

        assertSizeEventually(2, queue2);
        assertIterableEquals(queue2, new VersionedObject<>("item2", 2), new VersionedObject<>("item3", 3));
    }

    @Test
    public void testTakeInterruption() {
        Config config = getConfig()
                .setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "10000");

        HazelcastInstance instance = createHazelcastInstance(config);
        IQueue<Thread> queue = instance.getQueue(randomName());

        CountDownLatch takeLatch = new CountDownLatch(1);
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
        Config config = getConfig()
                .setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "10000");
        config.getQueueConfig("default").setMaxSize(1);

        HazelcastInstance instance = createHazelcastInstance(config);
        IQueue<VersionedObject<String>> queue = instance.getQueue(randomName());

        assertTrue("Expected queue.offer() to succeed", queue.offer(new VersionedObject<>("item")));

        CountDownLatch putLatch = new CountDownLatch(1);
        TestThread thread = new TestThread() {
            @Override
            public void doRun() throws Throwable {
                putLatch.countDown();
                queue.put(new VersionedObject<>("item"));
            }
        };
        thread.start();

        assertOpenEventually(putLatch);

        thread.interrupt();

        thread.assertFailsEventually(InterruptedException.class);
    }

    @Test
    public void test_continues_ownership_changes_does_not_leak_backup_memory() throws InterruptedException {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        // ownership changes happen on stable
        // instance and it shouldn't leak memory
        HazelcastInstance stableInstance = factory.newHazelcastInstance(config);

        String queueName = "itemQueue";
        IQueue<String> producer = stableInstance.getQueue(queueName);

        // initial offer
        producer.offer("item");
        for (int j = 0; j < 5; j++) {
            // start unreliable instance
            HazelcastInstance unreliableInstance = factory.newHazelcastInstance(config);

            // consume data in queue
            IQueue<String> consumer = unreliableInstance.getQueue(queueName);
            consumer.take();

            // intentional termination, we are not testing graceful shutdown.
            unreliableInstance.getLifecycleService().terminate();

            boolean itemAdded = producer.offer("item");


            assertEquals("Failed at step :" + j
                            + " (0 is first step) [itemAdded=" + itemAdded
                            + ", " + getQueueContainer(producer) + "]",
                    1, producer.size());
        }
    }

    private static QueueContainer getQueueContainer(IQueue<String> producer) {
        QueueService queueService = (QueueService) ((QueueProxyImpl) producer).getService();
        QueueContainer container = queueService.getExistingContainerOrNull(producer.getName());
        return container;
    }

    private HazelcastInstance[] createHazelcastInstances() {
        String configName = randomString();
        Config config = getConfig();
        config.getQueueConfig(configName).setMaxSize(100);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        return factory.newInstances(config);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
                .setPriorityComparatorClassName(comparatorClassName);
        return config;
    }
}
