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

package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MPSCQueueStressTest extends HazelcastTestSupport {

    private static final long DURATION_SECONDS = 30;

    private final AtomicBoolean stop = new AtomicBoolean();

    @Test
    public void test_singleProducer_block() throws Exception {
        test(1, null);
    }

    @Test
    public void test_twoProducers_block() throws Exception {
        test(2, null);
    }

    @Test
    public void test_multipleProducers_block() throws Exception {
        test(10, null);
    }

    @Test
    public void test_singleProducer_backoff() throws Exception {
        test(1, new BackoffIdleStrategy(100, 1000, 1000, MILLISECONDS.toNanos(1)));
    }

    @Test
    public void test_twoProducers_backoff() throws Exception {
        test(2, new BackoffIdleStrategy(100, 1000, 1000, MILLISECONDS.toNanos(1)));
    }

    @Test
    public void test_multipleProducers_backoff() throws Exception {
        test(10, new BackoffIdleStrategy(100, 1000, 1000, MILLISECONDS.toNanos(1)));
    }

    public void test(int producerCount, IdleStrategy idleStrategy) throws Exception {
        MPSCQueue<Item> queue = new MPSCQueue<Item>(idleStrategy);
        ConsumerThread consumers = new ConsumerThread(queue, producerCount);
        queue.setConsumerThread(consumers);
        consumers.start();

        List<ProducerThread> producers = new LinkedList<ProducerThread>();
        for (int k = 0; k < producerCount; k++) {
            ProducerThread producer = new ProducerThread(queue, k);
            producer.start();
            producers.add(producer);
        }
        sleepAndStop(stop, DURATION_SECONDS);

        long totalProduced = 0;
        for (ProducerThread producer : producers) {
            producer.assertSucceedsEventually();
            totalProduced += producer.itemCount;
        }
        consumers.assertSucceedsEventually();
        assertEquals(totalProduced, consumers.itemCount);
    }

    static class Item {

        private final long value;
        private final int producerId;

        Item(int producerId, long value) {
            this.value = value;
            this.producerId = producerId;
        }
    }

    class ProducerThread extends TestThread {

        private final MPSCQueue<Item> queue;
        private final int id;
        private long itemCount;

        ProducerThread(MPSCQueue<Item> queue, int id) {
            super("Producer-" + id);
            this.queue = queue;
            this.id = id;
        }

        @Override
        public void doRun() {
            Random random = new Random();
            while (!stop.get()) {
                itemCount++;
                queue.offer(new Item(id, itemCount));

                while (queue.size() > 100000) {
                    sleepMillis(random.nextInt(100));
                }

                if (random.nextInt(1000) == 0) {
                    sleepMillis(random.nextInt(100));
                }

                if (itemCount % 10000 == 0) {
                    System.out.println(getName() + " at " + itemCount);
                }
            }

            queue.offer(new Item(id, -1));

            System.out.println(getName() + " Done");
        }
    }

    class ConsumerThread extends TestThread {

        private final MPSCQueue<Item> queue;
        private final int producerCount;
        private final long[] producerSequence;
        private long itemCount;
        private volatile int completedProducers = 0;

        ConsumerThread(MPSCQueue<Item> queue, int producerCount) {
            super("Consumer");
            this.queue = queue;
            this.producerCount = producerCount;
            this.producerSequence = new long[producerCount];
        }

        @Override
        public void doRun() throws Exception {
            Random random = new Random();
            for (; ; ) {
                Item item = queue.take();

                if (item.value == -1) {
                    completedProducers++;
                    if (completedProducers == producerCount) {
                        break;
                    }
                } else {
                    itemCount++;
                    long last = producerSequence[item.producerId];
                    if (last + 1 != item.value) {
                        stop.set(true);
                        throw new RuntimeException();
                    }
                    producerSequence[item.producerId] = item.value;
                }

                if (itemCount % 10000 == 0) {
                    System.out.println(getName() + " at " + itemCount);
                }

                //System.out.println("Consumed: " + item);

                if (random.nextInt(1000) == 0) {
                    sleepMillis(random.nextInt(100));
                }
            }

            System.out.println(getName() + " Done");
        }
    }
}
