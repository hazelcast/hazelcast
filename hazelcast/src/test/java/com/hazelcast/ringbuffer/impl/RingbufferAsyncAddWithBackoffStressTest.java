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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.ringbuffer.OverflowPolicy.FAIL;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class RingbufferAsyncAddWithBackoffStressTest extends HazelcastTestSupport {
    private final AtomicBoolean stop = new AtomicBoolean();

    private Ringbuffer<Long> ringbuffer;

    @Test
    public void whenNoTTL() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(200 * 1000)
                .setTimeToLiveSeconds(0);
        test(ringbufferConfig);
    }

    @Test
    public void whenTTLEnabled() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(200 * 1000)
                .setTimeToLiveSeconds(2);
        test(ringbufferConfig);
    }

    @Test
    public void whenLongTTLAndSmallBuffer() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(1000)
                .setTimeToLiveSeconds(30);
        test(ringbufferConfig);
    }

    @Test(timeout = 15 * 60 * 1000)
    public void whenShortTTLAndBigBuffer() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(10 * 1000 * 1000)
                .setTimeToLiveSeconds(3);
        test(ringbufferConfig);
    }

    public void test(RingbufferConfig ringbufferConfig) throws Exception {
        Config config = new Config();
        config.addRingBufferConfig(ringbufferConfig);
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(config);
        ringbuffer = instances[0].getRingbuffer(ringbufferConfig.getName());

        ConsumeThread consumer1 = new ConsumeThread(1);
        consumer1.start();

        ConsumeThread consumer2 = new ConsumeThread(2);
        consumer2.start();

        sleepSeconds(2);

        ProduceThread producer = new ProduceThread();
        producer.start();

        sleepAndStop(stop, 3 * 60);
        System.out.println("Waiting for completion");

        producer.assertSucceedsEventually();
        consumer1.assertSucceedsEventually();
        consumer2.assertSucceedsEventually();

        System.out.println("producer.produced:" + producer.produced);

        assertEquals(producer.produced, consumer1.seq);
        assertEquals(producer.produced, consumer2.seq);
    }

    class ProduceThread extends TestThread {
        private volatile long produced;

        ProduceThread() {
            super("ProduceThread");
        }

        @Override
        public void onError(Throwable t) {
            stop.set(true);
        }

        @Override
        public void doRun() throws Throwable {
            long lastMs = System.currentTimeMillis();
            while (!stop.get()) {
                long sleepMs = 100;
                for (; ; ) {
                    long result = ringbuffer.addAsync(produced, FAIL).toCompletableFuture().get();
                    if (result != -1) {
                        break;
                    }
                    TimeUnit.MILLISECONDS.sleep(sleepMs);
                    sleepMs = sleepMs * 2;
                    if (sleepMs > 1000) {
                        sleepMs = 1000;
                    }
                }

                produced++;

                long now = System.currentTimeMillis();
                if (now > lastMs + 2000) {
                    lastMs = now;
                    System.out.println(getName() + " at " + produced);
                }
            }

            ringbuffer.add(Long.MIN_VALUE);
        }
    }

    class ConsumeThread extends TestThread {
        volatile long seq;

        ConsumeThread(int id) {
            super("ConsumeThread-" + id);
        }

        @Override
        public void onError(Throwable t) {
            stop.set(true);
        }

        @Override
        public void doRun() throws Throwable {
            long lastMs = System.currentTimeMillis();
            seq = ringbuffer.headSequence();

            for (; ; ) {
                Long item = null;

                while (item == null) {
                    try {
                        item = ringbuffer.readOne(seq);
                    } catch (StaleSequenceException e) {
                        // this consumer is used in a stress test and can fall behind the producer if it gets delayed
                        // by any reason. This is ok, just jump to the the middle of the ringbuffer.
                        System.out.println(getName() + " has fallen behind, catching up...");
                        final long tail = ringbuffer.tailSequence();
                        final long head = ringbuffer.headSequence();
                        seq = tail >= head ? ((tail + head) / 2) : head;
                    }
                }

                if (item.equals(Long.MIN_VALUE)) {
                    break;
                }

                assertEquals(new Long(seq), item);

                seq++;

                long now = System.currentTimeMillis();
                if (now > lastMs + 2000) {
                    lastMs = now;
                    System.out.println(getName() + " at " + seq);
                }
            }
        }
    }
}
