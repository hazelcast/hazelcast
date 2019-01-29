/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.lock;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LegacyClientConditionTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private ILock lock;

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
        lock = client.getLock(randomString());
    }

    @After
    public void tearDown() {
        lock.forceUnlock();
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testLockConditionSimpleUsage() throws InterruptedException {
        final String name = randomString();
        final ILock lock = client.getLock(name);
        final ICondition condition = lock.newCondition(randomString());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch threadGetTheLock = new CountDownLatch(1);

        Thread t = new Thread(new Runnable() {
            public void run() {
                lock.lock();
                threadGetTheLock.countDown();
                try {
                    if (lock.isLockedByCurrentThread()) {
                        count.incrementAndGet();
                    }
                    condition.await();
                    if (lock.isLockedByCurrentThread()) {
                        count.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    lock.unlock();
                }
            }
        });
        t.start();
        assertOpenEventually(threadGetTheLock);

        lock.lock();
        assertTrue(lock.isLocked());
        condition.signal();
        lock.unlock();
        t.join();
        assertEquals(2, count.get());
    }

    @Test
    public void testLockConditionSignalAll() throws InterruptedException {
        final String name = "testLockConditionSimpleUsage";
        final ILock lock = client.getLock(name);
        final ICondition condition = lock.newCondition(name + "c");
        final AtomicInteger count = new AtomicInteger(0);
        final int k = 50;

        final CountDownLatch awaitLatch = new CountDownLatch(k);
        final CountDownLatch finalLatch = new CountDownLatch(k);
        for (int i = 0; i < k; i++) {
            new Thread(new Runnable() {
                public void run() {
                    lock.lock();
                    try {
                        if (lock.isLockedByCurrentThread()) {
                            count.incrementAndGet();
                        }
                        awaitLatch.countDown();
                        condition.await();
                        if (lock.isLockedByCurrentThread()) {
                            count.incrementAndGet();
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        lock.unlock();
                        finalLatch.countDown();
                    }

                }
            }).start();
        }

        awaitLatch.await(1, TimeUnit.MINUTES);
        lock.lock();
        condition.signalAll();
        lock.unlock();
        finalLatch.await(1, TimeUnit.MINUTES);
        assertEquals(k * 2, count.get());
    }


    @Test(expected = DistributedObjectDestroyedException.class)
    public void testDestroyLockWhenOtherWaitingOnConditionAwait() {
        final ILock lock = client.getLock("testDestroyLockWhenOtherWaitingOnConditionAwait");
        final ICondition condition = lock.newCondition("condition");
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            public void run() {
                try {
                    latch.await(30, TimeUnit.SECONDS);
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lock.destroy();
            }
        }).start();

        lock.lock();
        try {
            latch.countDown();
            condition.await();
        } catch (InterruptedException e) {
        }
        lock.unlock();
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testIllegalConditionUsageWithoutAcquiringLock() {
        final ICondition condition = lock.newCondition("condition");
        try {
            condition.await();
        } catch (InterruptedException e) {
        }
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testIllegalConditionUsageSignalToNonAwaiter() {
        final ICondition condition = lock.newCondition("condition");
        condition.signal();
    }

    @Test
    public void testConditionUsage() throws InterruptedException {
        lock.lock();
        final ICondition condition = lock.newCondition("condition");
        condition.await(1, TimeUnit.SECONDS);
        lock.unlock();
    }

    @Test
    public void testAwaitNanos_remainingTime() throws InterruptedException {
        ICondition condition = lock.newCondition("condition");

        lock.lock();
        try {
            long timeout = 1000L;
            long remainingTimeout = condition.awaitNanos(timeout);
            assertTrue("Remaining timeout should be <= 0, but it's = " + remainingTimeout,
                    remainingTimeout <= 0);
        } finally {
            lock.unlock();
        }
    }
}
