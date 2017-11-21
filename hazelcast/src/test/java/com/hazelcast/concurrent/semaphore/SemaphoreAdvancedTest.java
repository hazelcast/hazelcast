/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.test.TimeConstants.MINUTE;
import static com.hazelcast.test.TimeConstants.SECOND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SemaphoreAdvancedTest extends HazelcastTestSupport {

    @Test(expected = IllegalStateException.class, timeout = 30 * SECOND)
    public void testAcquire_whenInstanceShutdown() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        hz.shutdown();
        semaphore.acquire();
    }


    @Test(timeout = 5 * MINUTE)
    public void testSemaphoreWithFailures() throws InterruptedException {
        final String semaphoreName = randomString();
        final int k = 4;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k + 1);
        final HazelcastInstance[] instances = factory.newInstances();
        final ISemaphore semaphore = instances[k].getSemaphore(semaphoreName);
        int initialPermits = 20;

        semaphore.init(initialPermits);

        for (int i = 0; i < k; i++) {
            int rand = (int) (Math.random() * 5) + 1;

            semaphore.acquire(rand);
            initialPermits -= rand;
            assertEquals(initialPermits, semaphore.availablePermits());
            semaphore.release(rand);
            initialPermits += rand;
            assertEquals(initialPermits, semaphore.availablePermits());

            instances[i].shutdown();

            semaphore.acquire(rand);
            initialPermits -= rand;
            assertEquals(initialPermits, semaphore.availablePermits());
            semaphore.release(rand);
            initialPermits += rand;
            assertEquals(initialPermits, semaphore.availablePermits());
        }
    }

    @Test(timeout = 5 * MINUTE)
    public void testSemaphoreWithFailuresAndJoin() {
        final String semaphoreName = randomString();
        final Config config = new Config().setProperty(GroupProperty.PARTITION_COUNT.getName(), "5");
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore1 = instance1.getSemaphore(semaphoreName);
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        assertTrue(semaphore1.init(0));

        spawn(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 2; i++) {
                    try {
                        semaphore1.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                countDownLatch.countDown();
            }
        });

        instance2.shutdown();
        semaphore1.release();

        final HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore3 = instance3.getSemaphore(semaphoreName);
        semaphore3.release();

        assertOpenEventually(countDownLatch);
    }

    @Test(timeout = 5 * MINUTE)
    public void testMutex() throws InterruptedException {
        final String semaphoreName = randomString();
        final int threadCount = 2;
        final HazelcastInstance[] instances = createHazelcastInstanceFactory(threadCount).newInstances();
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final int loopCount = 1000;
        final long[] counter = {0};

        assertTrue(instances[0].getSemaphore(semaphoreName).init(1));

        for (int i = 0; i < threadCount; i++) {
            final ISemaphore semaphore = instances[i].getSemaphore(semaphoreName);
            spawn(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < loopCount; j++) {
                        try {
                            semaphore.acquire();
                            sleepMillis((int) (Math.random() * 3));
                            counter[0]++;
                        } catch (InterruptedException e) {
                            return;
                        } finally {
                            semaphore.release();
                        }
                    }
                    latch.countDown();
                }
            });
        }

        assertOpenEventually(latch);
        assertEquals(loopCount * threadCount, counter[0]);
    }
}
