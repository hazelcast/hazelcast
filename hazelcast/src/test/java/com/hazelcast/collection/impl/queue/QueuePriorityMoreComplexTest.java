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
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class QueuePriorityMoreComplexTest extends HazelcastTestSupport {

    private IQueue<PriorityElement> queue;

    @Before
    public void before() {
        String queueName = randomString();
        Config config = new Config();
        config.getQueueConfig(queueName)
              .setPriorityComparatorClassName("com.hazelcast.collection.impl.queue.model.PriorityElementComparator");
        HazelcastInstance hz = createHazelcastInstance(config);
        queue = hz.getQueue(queueName);
    }

    @Test
    public void queue() {
        final PriorityElement element = new PriorityElement(false, 1);

        queue.offer(element);

        assertEquals(element, queue.poll());
        assertNull(queue.poll());
    }

    @Test
    public void queuePriorizing() {
        testWithSize(800);
    }

    private void testWithSize(final int size) {

        int count = 0;
        for (int i = 0; i < size; i++) {
            queue.offer(new PriorityElement(false, count));
            queue.offer(new PriorityElement(true, count));
            count++;
        }

        for (int i = 0; i < size; i++) {
            final PriorityElement dequeue = queue.poll();
            assertTrue("Highpriority first", dequeue.isHighPriority());
            assertEquals(Integer.valueOf(i), dequeue.getVersion());
        }
        for (int i = 0; i < size; i++) {
            final PriorityElement dequeue = queue.poll();
            assertFalse("Lowpriority afterwards", dequeue.isHighPriority());
            assertEquals(Integer.valueOf(i), dequeue.getVersion());
        }
        assertNull(queue.poll());
    }

    @Test
    public void size02000() {
        testWithSize(2000);
    }

    @Test
    public void size04000() {
        testWithSize(4000);
    }

    @Test
    public void size08000() {
        testWithSize(8000);
    }

    @Test
    public void size16000() {
        testWithSize(16000);
    }

    @Test
    public void size32000() {
        testWithSize(32000);
    }

    @Test
    public void size64000() {
        testWithSize(64000);
    }

    @Test
    public void queueConsistency() throws InterruptedException {

        int count = 0;
        for (int i = 0; i < 500; i++) {
            queue.offer(new PriorityElement(false, count));
            queue.offer(new PriorityElement(true, count));
            count++;
        }
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final ConcurrentSkipListSet<PriorityElement> tasks = new ConcurrentSkipListSet<>();
        final Semaphore sem = new Semaphore(-99);
        for (int i = 0; i < 100; i++) {
            threadPool.execute(new Runnable() {

                @Override
                public void run() {
                    PriorityElement task;
                    while ((task = queue.poll()) != null) {
                        tasks.add(task);
                    }
                    sem.release();
                }
            });
        }
        sem.acquire();
        assertEquals(500 * 2, tasks.size());
        assertNull(queue.poll());
    }

    @Test
    public void queueParallel() throws InterruptedException {

        final AtomicInteger enqueued = new AtomicInteger();
        final AtomicInteger dequeued = new AtomicInteger();
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final Semaphore sem = new Semaphore(-200);
        final int size = 1000;
        for (int i = 0; i <= 100; i++) {
            threadPool.execute(new Runnable() {

                @Override
                public void run() {
                    while (enqueued.get() < size) {
                        final int j = enqueued.incrementAndGet();
                        final boolean priority = j % 2 == 0;
                        final PriorityElement task = new PriorityElement(priority, j);
                        queue.offer(task);
                    }
                    sem.release();
                }
            });
            threadPool.execute(new Runnable() {

                @Override
                public void run() {
                    while (enqueued.get() > dequeued.get() || enqueued.get() < size) {
                        final PriorityElement dequeue = queue.poll();
                        if (dequeue != null) {
                            dequeued.incrementAndGet();
                        }
                    }
                    sem.release();
                }
            });
        }
        sem.acquire();
        assertEquals(enqueued.get(), dequeued.get());
        assertNull(queue.poll());
    }

    @Test
    public void offer_poll_and_offer_poll_again() {

        PriorityElement task = new PriorityElement(false, 1);

        assertEquals(null, queue.poll());
        assertTrue(queue.offer(task));
        assertEquals(task, queue.poll());
        assertEquals(null, queue.poll());
        assertTrue(queue.offer(task));
        assertEquals(task, queue.poll());
    }
}
