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

package com.hazelcast.cp.internal.datastructures.unsafe.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.ICondition;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ProducerConsumerConditionStressTest extends HazelcastTestSupport {

    private static volatile Object object;

    public static final long TEST_TIMEOUT_SECONDS = 10 * 60;
    public static final int ITERATIONS = 1000000;
    public static final int PRODUCER_COUNT = 2;
    public static final int CONSUMER_COUNT = 2;
    public static final int INSTANCE_COUNT = 1;

    @Test(timeout = 2 * TEST_TIMEOUT_SECONDS * 1000)
    public void test() {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(INSTANCE_COUNT).newInstances();
        HazelcastInstance hz = instances[0];
        ILock lock = hz.getLock(randomString());
        ICondition condition = lock.newCondition(randomString());

        ConsumerThread[] consumers = new ConsumerThread[CONSUMER_COUNT];
        for (int k = 0; k < consumers.length; k++) {
            ConsumerThread thread = new ConsumerThread(k, lock, condition);
            thread.start();
            consumers[k] = thread;
        }

        ProducerThread[] producers = new ProducerThread[PRODUCER_COUNT];
        for (int k = 0; k < producers.length; k++) {
            ProducerThread thread = new ProducerThread(k, lock, condition);
            thread.start();
            producers[k] = thread;
        }

        assertJoinable(TEST_TIMEOUT_SECONDS, producers);
        assertJoinable(TEST_TIMEOUT_SECONDS, consumers);

        for (TestThread consumer : consumers) {
            assertNull(consumer.throwable);
        }
        for (TestThread producer : producers) {
            assertNull(producer.throwable);
        }
    }

    abstract class TestThread extends Thread {
        private volatile Throwable throwable;

        TestThread(String name) {
            super(name);
        }

        public void run() {
            try {
                for (int k = 0; k < ITERATIONS; k++) {
                    runSingleIteration();

                    if (k % 100 == 0) {
                        System.out.println(getName() + " is at: " + k);
                    }
                }
            } catch (Throwable e) {
                this.throwable = e;
                e.printStackTrace();
            }
        }

        abstract void runSingleIteration() throws InterruptedException;
    }

    class ProducerThread extends TestThread {
        private final ILock lock;
        private final ICondition condition;

        ProducerThread(int id, ILock lock, ICondition condition) {
            super("ProducerThread-" + id);
            this.lock = lock;
            this.condition = condition;
        }


        void runSingleIteration() throws InterruptedException {
            lock.lock();

            try {
                while (object != null) {
                    condition.await();
                }
                object = "";
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    class ConsumerThread extends TestThread {

        private final ILock lock;
        private final ICondition condition;

        ConsumerThread(int id, ILock lock, ICondition condition) {
            super("ConsumerThread-" + id);
            this.lock = lock;
            this.condition = condition;
        }

        void runSingleIteration() throws InterruptedException {
            lock.lock();

            try {
                while (object == null) {
                    condition.await();
                }
                object = null;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
