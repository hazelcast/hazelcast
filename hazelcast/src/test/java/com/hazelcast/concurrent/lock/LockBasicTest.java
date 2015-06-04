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

package com.hazelcast.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.concurrent.lock.LockTestUtils.lockByOtherThread;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public abstract class LockBasicTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected ILock lock;

    @Before
    public void setup() {
        instances = newInstances();
        lock = newInstance();
    }

    protected ILock newInstance() {
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        return local.getLock(name);
    }

    protected abstract HazelcastInstance[] newInstances();

    // ======================== lock ==================================================

    @Test(timeout = 60000)
    public void testLock_whenNotLocked() {
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testLock_whenLockedBySelf() {
        lock.lock();
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testLock_whenLockedByOther() throws InterruptedException {
        lock.lock();
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
        assertTrue(lock.isLockedByCurrentThread());

        final CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread() {
            public void run() {
                lock.lock();
                latch.countDown();
            }
        };

        t.start();
        assertFalse(latch.await(3000, TimeUnit.MILLISECONDS));
    }

    // ======================== try lock ==============================================

    @Test(timeout = 60000)
    public void testTryLock_whenNotLocked() {
        boolean result = lock.tryLock();

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedBySelf() {
        lock.lock();

        boolean result = lock.tryLock();

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedByOther() {
        lockByOtherThread(lock);

        boolean result = lock.tryLock();

        assertFalse(result);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    // ======================== try lock with timeout ==============================================

    @Test(timeout = 60000)
    public void testTryLockTimeout_whenNotLocked() throws InterruptedException {
        boolean result = lock.tryLock(1, TimeUnit.SECONDS);

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockTimeout_whenLockedBySelf() throws InterruptedException {
        lock.lock();

        boolean result = lock.tryLock(1, TimeUnit.SECONDS);

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockTimeout_whenLockedByOtherAndTimeout() throws InterruptedException {
        lockByOtherThread(lock);

        boolean result = lock.tryLock(1, TimeUnit.SECONDS);

        assertFalse(result);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLockTimeout_whenLockedByOtherAndEventuallyAvailable() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                latch.countDown();
                sleepSeconds(1);
                lock.unlock();
            }
        }).start();
        latch.await();
        assertTrue(lock.tryLock(3, TimeUnit.SECONDS));

        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());

    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testTryLockTimeout_whenNullTimeout() throws InterruptedException {
        lock.tryLock(1, null);
    }
    
    // ======================== try lock with lease ==============================================

    @Test(timeout = 60000)
    public void testTryLockWithLeaseTimeout_whenNotLocked() throws InterruptedException {
        boolean result = lock.tryLock(1, 1, TimeUnit.SECONDS);

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockWithLeaseTimeout_whenLockedBySelf() throws InterruptedException {
        lock.lock();

        boolean result = lock.tryLock(1, 1, TimeUnit.SECONDS);

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockWithLeaseTimeout_whenLockedByOtherAndTimeout() throws InterruptedException {
        lockByOtherThread(lock);

        boolean result = lock.tryLock(1, 1, TimeUnit.SECONDS);

        assertFalse(result);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLockTimeoutWithLease_whenLockedByOtherAndEventuallyAvailable() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                latch.countDown();
                sleepSeconds(1);
                lock.unlock();
            }
        }).start();
        latch.await();
        assertTrue(lock.tryLock(1, 3, TimeUnit.SECONDS));

        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());

    }

    // ======================== unlock ==============================================

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlock_whenFree() {
        lock.unlock();
    }

    @Test(timeout = 60000)
    public void testUnlock_whenLockedBySelf() {
        lock.lock();

        lock.unlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testUnlock_whenReentrantlyLockedBySelf() {
        lock.lock();
        lock.lock();

        lock.unlock();

        assertTrue(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testUnlock_whenPendingLockOfOtherThread() throws InterruptedException {
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                latch.countDown();

            }
        });
        thread.start();

        lock.unlock();
        latch.await();

        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
    }

    @Test(timeout = 60000)
    public void testUnlock_whenLockedByOther() {
        lockByOtherThread(lock);

        try {
            lock.unlock();
            fail();
        } catch (IllegalMonitorStateException expected) {
        }

        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }


    // ======================== force unlock ==============================================

    @Test(timeout = 60000)
    public void testForceUnlock_whenLockNotOwned() {
        lock.forceUnlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testForceUnlock_whenOwnedByOtherThread() {
        lock.lock();

        lock.forceUnlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testForceUnlock_whenAcquiredByCurrentThread() {
        lock.lock();

        lock.forceUnlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testForceUnlock_whenAcquiredMultipleTimesByCurrentThread() {
        lock.lock();
        lock.lock();

        lock.forceUnlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }


    // ========================= lease time ==============================================

    @Test(expected = NullPointerException.class, timeout = 60000)
    public void testLockLeaseTime_whenNullTimeout() {
        lock.lock(1000, null);
    }

    @Test(timeout = 60000)
    public void testLockLeaseTime_whenLockFree() {
        lock.lock(1000, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 60000)
    public void testLockLeaseTime_whenLockAcquiredByOther() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread() {
            public void run() {
                lock.lock();
                latch.countDown();
                sleepMillis(500);
                lock.unlock();
            }
        }.start();

        latch.await();

        lock.lock(4000, TimeUnit.MILLISECONDS);

        assertTrue(lock.isLocked());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(lock.isLocked());
            }
        });
    }

    @Test(timeout = 60000)
    public void testLockLeaseTime_lockIsReleasedEventually() throws InterruptedException {
        lock.lock(1000, TimeUnit.MILLISECONDS);
        assertTrue(lock.isLocked());

        lock.lock();
        assertTrue(lock.isLocked());
    }


    // =======================================================================

    @Test(timeout = 60000)
    public void testTryLock_whenMultipleThreads() throws InterruptedException {
        final AtomicInteger atomicInteger = new AtomicInteger(0);

        lock.lock();

        Runnable tryLockRunnable = new Runnable() {
            public void run() {
                if (lock.tryLock()) {
                    atomicInteger.incrementAndGet();
                }
            }
        };

        Thread thread1 = new Thread(tryLockRunnable);
        thread1.start();
        thread1.join();
        assertEquals(0, atomicInteger.get());

        lock.unlock();
        Thread thread2 = new Thread(tryLockRunnable);
        thread2.start();
        thread2.join();

        assertEquals(1, atomicInteger.get());
        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
    }

    @Test(timeout = 60000)
    public void testLockUnlock() {
        assertFalse(lock.isLocked());

        lock.lock();
        assertTrue(lock.isLocked());
        lock.unlock();

        assertFalse(lock.isLocked());
    }

    @Test(timeout = 60000)
    public void testTryLock() {
        assertFalse(lock.isLocked());

        assertTrue(lock.tryLock());
        lock.unlock();
        assertFalse(lock.isLocked());
    }

    @Test(timeout = 60000, expected = DistributedObjectDestroyedException.class)
    public void testDestroyLockWhenOtherWaitingOnLock() throws InterruptedException {
        Thread t = new Thread(new Runnable() {
            public void run() {
                lock.lock();
            }
        });
        t.start();
        t.join();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lock.destroy();
            }
        }).start();

        lock.lock();
    }

    @Test
    public void testLockCount() throws Exception {
        lock.lock();
        assertEquals(1, lock.getLockCount());
        assertTrue(lock.tryLock());
        assertEquals(2, lock.getLockCount());

        lock.unlock();
        assertEquals(1, lock.getLockCount());
        assertTrue(lock.isLocked());

        lock.unlock();
        assertEquals(0, lock.getLockCount());
        assertFalse(lock.isLocked());
        assertEquals(-1L, lock.getRemainingLeaseTime());
    }

}
