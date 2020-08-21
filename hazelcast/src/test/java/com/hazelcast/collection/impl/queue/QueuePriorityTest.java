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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.PriorityElement;
import com.hazelcast.collection.impl.queue.model.PriorityElementComparator;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueuePriorityTest extends HazelcastTestSupport {
    private IQueue<PriorityElement> queue;

    @Before
    public void before() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName("com.hazelcast.collection.impl.queue.model.PriorityElementComparator");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        queue = instances[0].getQueue(generateKeyOwnedBy(instances[1]));
    }

    @Test
    public void testPriorityQueue_whenHighestOfferedSecond_thenTakeHighest() {

        PriorityElement elementLow = new PriorityElement(false, 1);
        PriorityElement elementHigh = new PriorityElement(true, 1);

        assertTrue(queue.offer(elementLow));
        assertTrue(queue.offer(elementHigh));
        assertEquals(2, queue.size());
        assertTrue(queue.poll().isHighPriority());
        assertFalse(queue.poll().isHighPriority());
        assertEquals(0, queue.size());
    }

    @Test
    public void testPriorityQueue_whenHighestOfferedFirst_thenTakeHighest() {

        PriorityElement elementLow = new PriorityElement(false, 1);
        PriorityElement elementHigh = new PriorityElement(true, 1);

        assertTrue(queue.offer(elementHigh));
        assertTrue(queue.offer(elementLow));
        assertEquals(2, queue.size());
        assertTrue(queue.poll().isHighPriority());
        assertFalse(queue.poll().isHighPriority());
        assertEquals(0, queue.size());
    }

    @Test
    public void testPriorityQueue_whenTwoHighest_thenTakeFirstVersion() {
        PriorityElement elementHigh1 = new PriorityElement(true, 1);
        PriorityElement elementHigh2 = new PriorityElement(true, 2);

        assertTrue(queue.offer(elementHigh1));
        assertTrue(queue.offer(elementHigh2));
        assertEquals(2, queue.size());
        assertEquals(1, queue.poll().getVersion());
        assertEquals(2, queue.poll().getVersion());
        assertEquals(0, queue.size());
    }

    @Test
    public void testPriorityQueue_whenTwoHighest_thenTakeFirstVersionAgain() {

        PriorityElement elementHigh1 = new PriorityElement(true, 1);
        PriorityElement elementHigh2 = new PriorityElement(true, 2);

        assertTrue(queue.offer(elementHigh2));
        assertTrue(queue.offer(elementHigh1));
        assertEquals(2, queue.size());
        assertEquals(1, queue.poll().getVersion());
        assertEquals(2, queue.poll().getVersion());
        assertEquals(0, queue.size());
    }

    @Test
    public void queue() {
        PriorityElement element = new PriorityElement(false, 1);
        queue.offer(element);
        assertEquals(element, queue.poll());
        assertNull(queue.poll());
    }

    @Test
    public void queuePrioritizing() {
        int count = 0;
        int elementCount = 100;
        for (int i = 0; i < elementCount; i++) {
            queue.offer(new PriorityElement(false, count));
            queue.offer(new PriorityElement(true, count));
            count++;
        }

        for (int i = 0; i < elementCount; i++) {
            PriorityElement dequeue = queue.poll();
            assertTrue("High priority first", dequeue.isHighPriority());
            assertEquals(i, dequeue.getVersion());
        }
        for (int i = 0; i < elementCount; i++) {
            PriorityElement dequeue = queue.poll();
            assertFalse("Low priority afterwards", dequeue.isHighPriority());
            assertEquals(i, dequeue.getVersion());
        }
        assertNull(queue.poll());
    }

    @Test
    public void queueConsistency() throws InterruptedException {
        int count = 0;
        for (int i = 0; i < 500; i++) {
            queue.offer(new PriorityElement(false, count));
            queue.offer(new PriorityElement(true, count));
            count++;
        }
        ExecutorService threadPool = Executors.newCachedThreadPool();
        ConcurrentSkipListSet<PriorityElement> tasks = new ConcurrentSkipListSet<>(new PriorityElementComparator());
        Semaphore sem = new Semaphore(-99);
        for (int i = 0; i < 100; i++) {
            threadPool.execute(() -> {
                PriorityElement task;
                while ((task = queue.poll()) != null) {
                    tasks.add(task);
                }
                sem.release();
            });
        }
        sem.acquire();
        assertEquals(500 * 2, tasks.size());
        assertNull(queue.poll());
    }

    @Test
    public void queueParallel() throws InterruptedException {
        AtomicInteger enqueued = new AtomicInteger();
        AtomicInteger dequeued = new AtomicInteger();
        ExecutorService threadPool = Executors.newCachedThreadPool();
        Semaphore sem = new Semaphore(-200);
        int size = 1000;
        for (int i = 0; i <= 100; i++) {
            threadPool.execute(() -> {
                while (enqueued.get() < size) {
                    int j = enqueued.incrementAndGet();
                    boolean priority = j % 2 == 0;
                    PriorityElement task = new PriorityElement(priority, j);
                    queue.offer(task);
                }
                sem.release();
            });
            threadPool.execute(() -> {
                while (enqueued.get() > dequeued.get() || enqueued.get() < size) {
                    PriorityElement dequeue = queue.poll();
                    if (dequeue != null) {
                        dequeued.incrementAndGet();
                    }
                }
                sem.release();
            });
        }
        sem.acquire();
        assertEquals(enqueued.get(), dequeued.get());
        assertNull(queue.poll());
    }

    @Test
    public void offer_poll_and_offer_poll_again() {
        PriorityElement task = new PriorityElement(false, 1);
        assertNull(queue.poll());
        assertTrue(queue.offer(task));
        assertEquals(task, queue.poll());
        assertNull(queue.poll());
        assertTrue(queue.offer(task));
        assertEquals(task, queue.poll());
    }
}
