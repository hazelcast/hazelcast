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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class OperationQueueImplStressTest extends HazelcastTestSupport {

    private static final Object POISON_PILL = new Object();

    private final OperationQueueImpl queue = new OperationQueueImpl();
    private final AtomicBoolean stop = new AtomicBoolean();

    @Test
    public void testMultipleConsumers() {
        int testDurationSeconds = 10;

        ProducerThread producer = new ProducerThread();
        ConsumerThread consumer1 = new ConsumerThread(1);
        ConsumerThread consumer2 = new ConsumerThread(2);

        producer.start();
        consumer1.start();
        consumer2.start();

        sleepAndStop(stop, testDurationSeconds);

        producer.assertSucceedsEventually();
        consumer1.assertSucceedsEventually();
        consumer2.assertSucceedsEventually();

        long consumed = consumer1.consumed + consumer2.consumed;
        assertEquals(producer.produced, consumed);
    }

    private class ProducerThread extends TestThread {
        private volatile long produced;

        ProducerThread() {
            super("ProducerThread");
        }

        @Override
        public void doRun() throws Throwable {
            Random random = new Random();
            while (!stop.get()) {
                if (random.nextInt(5) == 0) {
                    queue.add("foo", true);
                } else {
                    queue.add("foo", false);
                }
                produced++;
            }

            for (int k = 0; k < 100; k++) {
                queue.add(POISON_PILL, false);
            }
        }
    }

    private class ConsumerThread extends TestThread {

        volatile long consumed = 0;

        private ConsumerThread(int id) {
            super("ConsumerThread-" + id);
        }

        @Override
        public void doRun() throws Throwable {
            for (; ; ) {
                Object item = queue.take(false);
                if (item == POISON_PILL) {
                    break;
                }
                consumed++;
            }
        }
    }
}
