/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientLockTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testLock() throws Exception {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (!lock.tryLock()) {
                    latch.countDown();
                }
            }
        }.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        lock.forceUnlock();
    }

    @Test
    public void testLockTtl() throws Exception {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        lock.lock(3, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread() {
            public void run() {
                if (!lock.tryLock()) {
                    latch.countDown();
                }
                try {
                    if (lock.tryLock(5, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    ignore(e);
                }
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        lock.forceUnlock();
    }

    @Test
    public void testTryLock() throws Exception {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        assertTrue(lock.tryLock(2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (!lock.tryLock(2, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    ignore(e);
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertTrue(lock.isLocked());

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (lock.tryLock(20, TimeUnit.SECONDS)) {
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    ignore(e);
                }
            }
        }.start();
        Thread.sleep(1000);
        lock.unlock();
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(lock.isLocked());
        lock.forceUnlock();
    }


    @Test
    public void testTryLockwithZeroTTL() throws Exception {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        boolean lockWithZeroTTL = lock.tryLock(0, TimeUnit.SECONDS);
        assertTrue(lockWithZeroTTL);
    }

    @Test
    public void testTryLockwithZeroTTLWithExistingLock() throws Exception {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (!lock.tryLock(0, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                }
            }
        }.start();
        assertOpenEventually(latch);
        lock.forceUnlock();
    }

    @Test
    public void testForceUnlock() throws Exception {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                lock.forceUnlock();
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertFalse(lock.isLocked());
    }

    @Test
    public void testStats() throws InterruptedException {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        lock.lock();
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());

        lock.unlock();
        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
        assertEquals(-1L, lock.getRemainingLeaseTime());

        lock.lock(1, TimeUnit.MINUTES);
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        assertTrue(lock.getRemainingLeaseTime() > 1000 * 30);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                assertTrue(lock.isLocked());
                assertFalse(lock.isLockedByCurrentThread());
                assertEquals(1, lock.getLockCount());
                assertTrue(lock.getRemainingLeaseTime() > 1000 * 30);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void testObtainLock_FromDifferentClients() throws InterruptedException {
        factory.newHazelcastInstance();
        String name = randomName();

        HazelcastInstance clientA = factory.newHazelcastClient();
        ILock lockA = clientA.getLock(name);
        lockA.lock();

        HazelcastInstance clientB = factory.newHazelcastClient();
        ILock lockB = clientB.getLock(name);
        boolean lockObtained = lockB.tryLock();

        assertFalse("Lock obtained by 2 client ", lockObtained);
    }

    @Test(timeout = 60000)
    public void testTryLockLeaseTime_whenLockFree() throws InterruptedException {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        boolean isLocked = lock.tryLock(1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);
    }

    @Test(timeout = 60000)
    public void testTryLockLeaseTime_whenLockAcquiredByOther() throws InterruptedException {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        Thread thread = new Thread() {
            public void run() {
                lock.lock();
            }
        };
        thread.start();
        thread.join();

        boolean isLocked = lock.tryLock(1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertFalse(isLocked);
    }

    @Test
    public void testTryLockLeaseTime_lockIsReleasedEventually() throws InterruptedException {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        lock.tryLock(1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(lock.isLocked());
            }
        }, 30);
    }

    @Test
    public void testMaxLockLeaseTime() {
        Config config = new Config();
        config.setProperty(GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS, "1");

        factory.newHazelcastInstance(config);

        HazelcastInstance hz = factory.newHazelcastClient();
        final ILock lock = hz.getLock(randomName());

        lock.lock();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse("Lock should be released after lease expires!", lock.isLocked());
            }
        }, 30);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockFail_whenGreaterThanMaxLeaseTimeUsed() {
        Config config = new Config();
        config.setProperty(GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS, "1");

        factory.newHazelcastInstance(config);

        HazelcastInstance hz = factory.newHazelcastClient();
        ILock lock = hz.getLock(randomName());

        lock.lock(10, TimeUnit.SECONDS);
    }
}
