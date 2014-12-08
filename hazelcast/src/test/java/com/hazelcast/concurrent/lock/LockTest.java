/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ILock;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.concurrent.lock.LockTestUtils.lockByOtherThread;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LockTest extends HazelcastTestSupport {


    // ======================== lock ==================================================

    @Test(timeout = 60000)
    public void testLock_whenNotLocked() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());

        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testLock_whenLockedBySelf() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());

        lock.lock();
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testLock_whenLockedByOther() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

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
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());

        boolean result = lock.tryLock();

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedBySelf() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        lock.lock();

        boolean result = lock.tryLock();

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedByOther() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
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
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());

        boolean result = lock.tryLock(1, TimeUnit.SECONDS);

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockTimeout_whenLockedBySelf() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        lock.lock();

        boolean result = lock.tryLock(1, TimeUnit.SECONDS);

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockTimeout_whenLockedByOtherAndTimeout() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        lockByOtherThread(lock);

        boolean result = lock.tryLock(1, TimeUnit.SECONDS);

        assertFalse(result);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testTryLockTimeout_whenLockedByOtherAndEventuallyAvailable() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

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
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock("testTryLockTimeout_whenNullTimeout");
        lock.tryLock(1, null);
    }

    // ======================== unlock ==============================================

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlock_whenFree() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock("testIllegalUnlock");
        lock.unlock();
    }

    @Test(timeout = 60000)
    public void testUnlock_whenLockedBySelf() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        lock.lock();

        lock.unlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testUnlock_whenReentrantlyLockedBySelf() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock("testUnlock_whenReentrantlyLockedBySelf");
        lock.lock();
        lock.lock();

        lock.unlock();

        assertTrue(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testUnlock_whenPendingLockOfOtherThread() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

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
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
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
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());

        lock.forceUnlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testForceUnlock_whenOwnedByOtherThread() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        lock.lock();

        lock.forceUnlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testForceUnlock_whenAcquiredByCurrentThread() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        lock.lock();

        lock.forceUnlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testForceUnlock_whenAcquiredMultipleTimesByCurrentThread() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        lock.lock();
        lock.lock();

        lock.forceUnlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }


    // ========================= lease time ==============================================

    @Test(expected = NullPointerException.class, timeout = 60000)
    public void testLockLeaseTime_whenNullTimeout() {
        final HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

        lock.lock(1000, null);
    }

    @Test(timeout = 60000)
    public void testLockLeaseTime_whenLockFree() {
        final HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

        lock.lock(1000, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 60000)
    public void testLockLeaseTime_whenLockAcquiredByOther() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

        final CountDownLatch latch0 = new CountDownLatch(1);
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread() {
            public void run() {
                lock.lock();
                latch0.countDown();
                sleepMillis(500);
                lock.unlock();
            }
        }.start();

        latch0.await();

        lock.lock(1000, TimeUnit.MILLISECONDS);

        assertTrue(lock.isLocked());
        sleepSeconds(2);
        assertFalse(lock.isLocked());
    }

    @Test(timeout = 60000)
    public void testLockLeaseTime_lockIsReleasedEventually() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

        lock.lock(1000, TimeUnit.MILLISECONDS);
        assertTrue(lock.isLocked());

        lock.lock();
        assertTrue(lock.isLocked());
    }


    // =======================================================================

    @Test(timeout = 60000)
    public void testTryLock_whenMultipleThreads() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final ILock lock = instance.getLock("testSimpleUsage");

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
        HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

        assertFalse(lock.isLocked());

        lock.lock();
        assertTrue(lock.isLocked());
        lock.unlock();

        assertFalse(lock.isLocked());
    }

    @Test(timeout = 60000)
    public void testTryLock() {
        HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock(randomString());

        assertFalse(lock.isLocked());

        assertTrue(lock.tryLock());
        lock.unlock();
        assertFalse(lock.isLocked());
    }

    @Test(timeout = 60000, expected = DistributedObjectDestroyedException.class)
    public void testDestroyLockWhenOtherWaitingOnLock() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance();
        final ILock lock = instance.getLock("testLockDestroyWhenWaitingLock");
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

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testShutDownNodeWhenOtherWaitingOnLockLocalKey() throws InterruptedException {
        testShutDownNodeWhenOtherWaitingOnLock(true);
    }

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testShutDownNodeWhenOtherWaitingOnLockRemoteKey() throws InterruptedException {
        testShutDownNodeWhenOtherWaitingOnLock(false);
    }

    private void testShutDownNodeWhenOtherWaitingOnLock(boolean localKey) throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();
        warmUpPartitions(instance2, instance);

        final String key;
        if (localKey) {
            key = generateKeyOwnedBy(instance);
        } else {
            key = generateKeyNotOwnedBy(instance);
        }

        final ILock lock = instance.getLock(key);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                lock.lock();
            }
        });
        thread.start();
        thread.join();
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                instance.shutdown();
            }
        }).start();
        lock.lock();
    }


    @Test(timeout = 100000)
    public void testLockEvictionLocalKey() throws Exception {
        testLockEviction(true);
    }

    @Test(timeout = 100000)
    public void testLockEvictionRemoteKey() throws Exception {
        testLockEviction(false);
    }

    private void testLockEviction(boolean localKey) throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        warmUpPartitions(instance2, instance1);

        final String key;
        if (localKey) {
            key = generateKeyOwnedBy(instance1);
        } else {
            key = generateKeyNotOwnedBy(instance1);
        }

        final ILock lock = instance1.getLock(key);
        lock.lock(10, TimeUnit.SECONDS);
        assertTrue(lock.getRemainingLeaseTime() > 0);
        assertTrue(lock.isLocked());

        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance2.getLock(key);
                lock.lock();
                latch.countDown();

            }
        });
        t.start();
        assertTrue(latch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void testLockCount() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();

        ILock lock = instance.getLock(randomString());
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


    /**
     * Test for issue #39
     */
    @Test
    public void testIsLocked() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();
        final String key = "testLockIsLocked";
        final ILock lock = h1.getLock(key);
        final ILock lock2 = h2.getLock(key);

        assertFalse(lock.isLocked());
        assertFalse(lock2.isLocked());
        lock.lock();
        assertTrue(lock.isLocked());
        assertTrue(lock2.isLocked());

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        Thread thread = new Thread(new Runnable() {
            public void run() {
                ILock lock3 = h3.getLock(key);
                assertTrue(lock3.isLocked());
                try {
                    latch2.countDown();
                    while (lock3.isLocked()) {
                        Thread.sleep(100);
                    }
                    latch.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
        latch2.await(3, TimeUnit.SECONDS);
        Thread.sleep(500);
        lock.unlock();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    //todo:   what does isLocked2 test?
    @Test(timeout = 60000)
    public void testIsLocked2() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final String key = randomString();

        final ILock lock = instance1.getLock(key);
        lock.lock();
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());

        assertTrue(lock.tryLock());
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());

        final AtomicBoolean result = new AtomicBoolean();
        final Thread thread = new Thread() {
            public void run() {
                result.set(lock.isLockedByCurrentThread());
            }
        };
        thread.start();
        thread.join();
        assertFalse(result.get());

        lock.unlock();
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());
    }

    @Test(timeout = 60000)
    public void testLockInterruption() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "5000");
        final HazelcastInstance hz = createHazelcastInstance(config);

        final Lock lock = hz.getLock("testLockInterruption2");
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    lock.tryLock(60, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    latch.countDown();
                }
            }
        });
        lock.lock();
        t.start();
        Thread.sleep(2000);
        t.interrupt();
        assertTrue("tryLock() is not interrupted!", latch.await(30, TimeUnit.SECONDS));
        lock.unlock();
        assertTrue("Could not acquire lock!", lock.tryLock());
    }


    // ====================== tests to make sure the lock can deal with cluster member failure ====================

    @Test(timeout = 100000)
    public void testLockOwnerDies() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance lockOwner = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();

        final String name = randomString();
        final ILock lock = lockOwner.getLock(name);
        lock.lock();
        assertTrue(lock.isLocked());
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance1.getLock(name);
                lock.lock();
                latch.countDown();

            }
        });
        t.start();
        lockOwner.shutdown();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 100000)
    public void testKeyOwnerDies() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final HazelcastInstance keyOwner = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        warmUpPartitions(keyOwner, instance1, instance2);
        final String key = generateKeyOwnedBy(keyOwner);
        final ILock lock1 = instance1.getLock(key);
        lock1.lock();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance2.getLock(key);
                lock.lock();
                latch.countDown();
            }
        }).start();

        Thread.sleep(1000);
        keyOwner.shutdown();
        assertTrue(lock1.isLocked());
        assertTrue(lock1.isLockedByCurrentThread());
        assertTrue(lock1.tryLock());
        lock1.unlock();
        lock1.unlock();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 100000)
    public void testScheduledLockActionForDeadMember() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final ILock lock1 = h1.getLock("default");
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final ILock lock2 = h2.getLock("default");

        assertTrue(lock1.tryLock());

        final AtomicBoolean error = new AtomicBoolean(false);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    lock2.lock();
                    error.set(true);
                } catch (Throwable ignored) {
                }
            }
        });
        thread.start();
        Thread.sleep(5000);

        assertTrue(lock1.isLocked());
        h2.shutdown();
        thread.join(10000);
        assertFalse(thread.isAlive());
        assertFalse(error.get());

        assertTrue(lock1.isLocked());
        lock1.unlock();
        assertFalse(lock1.isLocked());
        assertTrue(lock1.tryLock());
    }

    @Test
    public void testLockInterruptibly() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "5000");
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        final ILock lock = h1.getLock(randomString());
        final CountDownLatch latch = new CountDownLatch(1);
        lock.lock();
        Thread t = new Thread() {
            public void run() {
                try {
                    lock.lockInterruptibly();
                } catch (InterruptedException e) {
                    latch.countDown();
                }
            }
        };
        t.start();
        sleepMillis(5000);
        t.interrupt();
        assertTrue(latch.await(15, TimeUnit.SECONDS));
    }
}
