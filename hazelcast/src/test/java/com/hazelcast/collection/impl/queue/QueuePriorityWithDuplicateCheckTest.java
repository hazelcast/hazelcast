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
import com.hazelcast.collection.impl.queue.model.PriorityElement;
import com.hazelcast.collection.impl.queue.model.PriorityElementComparator;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueuePriorityWithDuplicateCheckTest extends HazelcastTestSupport {

    private static final ILogger LOG = Logger.getLogger(QueuePriorityWithDuplicateCheckTest.class);
    private PriorityElementTaskQueueImpl queue;
    private ExecutorService threadPool;

    @Before
    public void before() {
        Config config = smallInstanceConfig();
        String queueName = randomName();
        String mapName = randomName();

        config.getQueueConfig(queueName)
                .setPriorityComparatorClassName("com.hazelcast.collection.impl.queue.model.PriorityElementComparator");
        HazelcastInstance hz = createHazelcastInstance(config);
        queue = new PriorityElementTaskQueueImpl(hz.getQueue(queueName), hz.getMap(mapName));
        threadPool = Executors.newCachedThreadPool();
    }

    @After
    public void cleanup() {
        if (threadPool != null) {
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                fail("InterruptedException");
            } finally {
                threadPool.shutdownNow();
            }
        }
    }

    @Test
    public void queue() {
        PriorityElement element = new PriorityElement(false, 1);
        queue.enqueue(element);
        assertEquals(element, queue.dequeue());
        assertNull(queue.dequeue());
    }

    @Test
    public void queueTriple() {
        PriorityElement element = new PriorityElement(false, 1);
        queue.enqueue(element);
        queue.enqueue(element);
        queue.enqueue(element);
        assertEquals(element, queue.dequeue());
        assertNull(queue.dequeue());
    }

    @Test
    public void queuePrioritizing() {
        int size = 100;
        int count = 0;
        for (int i = 0; i < size; i++) {
            queue.enqueue(new PriorityElement(false, count));
            queue.enqueue(new PriorityElement(true, count));
            count++;
        }
        for (int i = 0; i < size; i++) {
            PriorityElement dequeue = queue.dequeue();
            assertTrue("High priority first", dequeue.isHighPriority());
        }
        for (int i = 0; i < size; i++) {
            PriorityElement dequeue = queue.dequeue();
            assertFalse("Low priority afterwards", dequeue.isHighPriority());
        }
        assertNull(queue.dequeue());
    }

    @Test
    public void queueConsistency() {
        int count = 0;
        for (int i = 0; i < 500; i++) {
            queue.enqueue(new PriorityElement(false, count));
            queue.enqueue(new PriorityElement(true, count));
            count++;
        }
        ConcurrentSkipListSet<PriorityElement> tasks = new ConcurrentSkipListSet<>(new PriorityElementComparator());
        CountDownLatch latch = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            threadPool.execute(() -> {
                PriorityElement task;
                while ((task = queue.dequeue()) != null) {
                    tasks.add(task);
                }
                latch.countDown();
            });
        }
        assertOpenEventually(latch);
        assertEquals(500 * 2, tasks.size());
    }

    @Test
    public void queueParallel() {
        AtomicInteger enqueued = new AtomicInteger();
        AtomicInteger dequeued = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(200);
        int size = 1000;
        for (int i = 0; i < 100; i++) {
            threadPool.execute(() -> {
                while (enqueued.get() < size) {
                    int j = enqueued.incrementAndGet();
                    boolean priority = j % 2 == 0;
                    PriorityElement task = new PriorityElement(priority, j);
                    queue.enqueue(task);
                }
                latch.countDown();
            });
            threadPool.execute(() -> {
                while (enqueued.get() > dequeued.get() || enqueued.get() < size) {
                    PriorityElement dequeue = queue.dequeue();
                    if (dequeue != null) {
                        dequeued.incrementAndGet();
                    }
                }
                latch.countDown();
            });
        }
        assertOpenEventually(latch);
        assertEquals(enqueued.get(), dequeued.get());
        assertNull(queue.dequeue());
    }

    @Test
    public void offer_poll_and_offer_poll_again() {
        PriorityElement task = new PriorityElement(false, 1);
        assertNull(queue.dequeue());
        assertTrue(queue.enqueue(task));
        assertEquals(task, queue.dequeue());
        assertNull(queue.dequeue());
        assertTrue(queue.enqueue(task));
        assertEquals(task, queue.dequeue());
    }


    static class PriorityElementTaskQueueImpl {
        private final IQueue<PriorityElement> queue;
        private final IMap<PriorityElement, PriorityElement> map;

        PriorityElementTaskQueueImpl(IQueue<PriorityElement> queue,
                                     IMap<PriorityElement, PriorityElement> map) {
            this.queue = queue;
            this.map = map;
        }

        public boolean enqueue(PriorityElement task) {
            try {
                PriorityElement previousValue = map.get(task);

                if (previousValue != null) {
                    return false;
                }

                boolean added = queue.offer(task);
                if (added) {
                    map.put(task, task);
                }
                return added;
            } catch (Exception e) {
                LOG.warning("Unable to write to priorityQueue: " + e);
                return false;
            }

        }

        public PriorityElement dequeue() {
            try {
                PriorityElement element = queue.poll();
                if (element != null) {
                    map.remove(element);
                }
                return element;
            } catch (Exception e) {
                LOG.warning("Unable to read from priorityQueue: " + e);
                return null;
            }
        }

        public void clear() {
            try {
                queue.clear();
                map.clear();
            } catch (Exception e) {
                LOG.warning("Unable to clear priorityQueue", e);
            }
        }
    }
}
