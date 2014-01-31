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
import com.hazelcast.core.*;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.*;


/**
 * User: sancar
 * Date: 2/18/13
 * Time: 5:12 PM
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LockTest extends HazelcastTestSupport {

    @Test
    public void testSimpleUsage() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final Config config = new Config();
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final ILock lock = instance.getLock("testSimpleUsage");
        Assert.assertEquals("testSimpleUsage", lock.getName());

        final Runnable tryLockRunnable = new Runnable() {
            public void run() {
                if (lock.tryLock())
                    atomicInteger.incrementAndGet();
            }
        };

        final Runnable lockRunnable = new Runnable() {
            public void run() {
                lock.lock();
            }
        };

        Assert.assertEquals(false, lock.isLocked());
        lock.lock();
        Assert.assertEquals(true, lock.isLocked());
        Assert.assertEquals(true, lock.tryLock());
        lock.unlock();

        Thread thread1 = new Thread(tryLockRunnable);
        thread1.start();
        thread1.join();
        Assert.assertEquals(0, atomicInteger.get());

        lock.unlock();
        Thread thread2 = new Thread(tryLockRunnable);
        thread2.start();
        thread2.join();
        Assert.assertEquals(1, atomicInteger.get());
        Assert.assertEquals(true, lock.isLocked());
        lock.forceUnlock();

        Thread thread3 = new Thread(lockRunnable);
        thread3.start();
        thread3.join();
        Assert.assertEquals(true, lock.isLocked());
        Assert.assertEquals(false, lock.tryLock(2, TimeUnit.SECONDS));

        Thread thread4 = new Thread(lockRunnable);
        thread4.start();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Assert.assertEquals(true, lock.isLocked());
            }
        });

        lock.forceUnlock();
        thread4.join();
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testDestroyLockWhenOtherWaitingOnLock() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(new Config());
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
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(new Config());
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(new Config());
        warmUpPartitions(instance2, instance);

        final String key;
        if (localKey) {
            key = generateKeyOwnedBy(instance);
        } else {
            key = generateKeyNotOwnedBy(instance);
        }

        final ILock lock = instance.getLock(key);
        final Thread thread = new Thread(new Runnable() {
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
                instance.getLifecycleService().shutdown();
            }
        }).start();
        lock.lock();
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testIllegalUnlock() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(new Config());
        final ILock lock = instance.getLock("testIllegalUnlock");
        lock.unlock();
    }

    @Test(timeout = 100000)
    public void testLockOwnerDies() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();
        final HazelcastInstance lockOwner = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final String name = "testLockOwnerDies";
        final ILock lock = lockOwner.getLock(name);
        lock.lock();
        Assert.assertTrue(lock.isLocked());
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance1.getLock(name);
                lock.lock();
                latch.countDown();

            }
        });
        t.start();
        lockOwner.getLifecycleService().shutdown();
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 100000)
    public void testKeyOwnerDies() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final Config config = new Config();
        final HazelcastInstance keyOwner = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

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
        keyOwner.getLifecycleService().shutdown();
        Assert.assertTrue(lock1.isLocked());
        Assert.assertTrue(lock1.isLockedByCurrentThread());
        Assert.assertTrue(lock1.tryLock());
        lock1.unlock();
        lock1.unlock();
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
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
        final Config config = new Config();

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

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
        Assert.assertTrue(lock.isLocked());

        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance2.getLock(key);
                lock.lock();
                latch.countDown();

            }
        });
        t.start();
        Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void testLockCount() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final int key = new Random().nextInt();

        final ILock lock = instance1.getLock(key);
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

    @Test
    public void testIsLocked2() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final int key = new Random().nextInt();

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


    @Test
    public void testLockConditionSimpleUsage() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();
        final String name = "testLockConditionSimpleUsage";
        final ILock lock = nodeFactory.newHazelcastInstance(config).getLock(name);
        final ICondition condition = lock.newCondition(name + "c");
        final AtomicInteger count = new AtomicInteger(0);

        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    lock.lock();
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
        Thread.sleep(1000);

        final ILock lock1 = nodeFactory.newHazelcastInstance(config).getLock(name);
        final ICondition condition1 = lock1.newCondition(name + "c");
        Assert.assertEquals(false, lock1.isLocked());
        lock1.lock();
        Assert.assertEquals(true, lock1.isLocked());
        condition1.signal();
        lock1.unlock();
        t.join();
        Assert.assertEquals(2, count.get());
    }

    @Test
    public void testLockConditionSignalAll() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();
        final String name = "testLockConditionSimpleUsage";
        final ILock lock = nodeFactory.newHazelcastInstance(config).getLock(name);
        final ICondition condition = lock.newCondition(name + "c");
        final AtomicInteger count = new AtomicInteger(0);
        final int k = 50;

        final CountDownLatch awaitLatch = new CountDownLatch(k);
        final CountDownLatch finalLatch = new CountDownLatch(k);
        for (int i = 0; i < k; i++) {
            new Thread(new Runnable() {
                public void run() {
                    try {
                        lock.lock();
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
        final ILock lock1 = nodeFactory.newHazelcastInstance(config).getLock(name);
        final ICondition condition1 = lock1.newCondition(name + "c");
        lock1.lock();
        condition1.signalAll();
        lock1.unlock();
        finalLatch.await(1, TimeUnit.MINUTES);
        Assert.assertEquals(k * 2, count.get());
    }

    @Test
    public void testLockConditionSignalAllShutDownKeyOwner() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();
        final String name = "testLockConditionSignalAllShutDownKeyOwner";
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        final AtomicInteger count = new AtomicInteger(0);
        final int size = 50;
        int k = 0;
        final HazelcastInstance keyOwner = nodeFactory.newHazelcastInstance(config);
        while (!keyOwner.getCluster().getLocalMember().equals(instance.getPartitionService().getPartition(++k).getOwner())) {
            Thread.sleep(10);
        }

        final ILock lock = instance.getLock(k);
        final ICondition condition = lock.newCondition(name);

        final CountDownLatch awaitLatch = new CountDownLatch(size);
        final CountDownLatch finalLatch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            new Thread(new Runnable() {
                public void run() {
                    lock.lock();
                    try {
                        awaitLatch.countDown();
                        condition.await();
                        Thread.sleep(5);
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

        final ILock lock1 = keyOwner.getLock(k);
        final ICondition condition1 = lock1.newCondition(name);
        awaitLatch.await(1, TimeUnit.MINUTES);
        lock1.lock();
        condition1.signalAll();
        lock1.unlock();
        keyOwner.getLifecycleService().shutdown();

        finalLatch.await(2, TimeUnit.MINUTES);
        Assert.assertEquals(size, count.get());
    }

    @Test(timeout = 100000)
    public void testKeyOwnerDiesOnCondition() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final Config config = new Config();
        final HazelcastInstance keyOwner = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        int k = 0;
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        while (keyOwner.getCluster().getLocalMember().equals(instance1.getPartitionService().getPartition(k++).getOwner())) {
            Thread.sleep(10);
        }

        final int key = k;
        final ILock lock1 = instance1.getLock(key);
        final String name = "testKeyOwnerDiesOnCondition";
        final ICondition condition1 = lock1.newCondition(name);

        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance2.getLock(key);
                final ICondition condition = lock.newCondition(name);
                lock.lock();
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
                atomicInteger.incrementAndGet();
            }
        });
        t.start();
        Thread.sleep(1000);
        lock1.lock();
        keyOwner.getLifecycleService().shutdown();

        condition1.signal();

        lock1.unlock();
        Thread.sleep(1000);
        t.join();
        Assert.assertEquals(1, atomicInteger.get());

    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testDestroyLockWhenOtherWaitingOnConditionAwait() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(new Config());
        final ILock lock = instance.getLock("testDestroyLockWhenOtherWaitingOnConditionAwait");
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

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testShutDownNodeWhenOtherWaitingOnConditionAwait() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(new Config());
        nodeFactory.newHazelcastInstance(new Config());
        final String name = "testShutDownNodeWhenOtherWaitingOnConditionAwait";
        final ILock lock = instance.getLock(name);
        final ICondition condition = lock.newCondition("s");
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            public void run() {
                try {
                    latch.await(1, TimeUnit.MINUTES);
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                instance.getLifecycleService().shutdown();
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
    public void testIllegalConditionUsage1() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(new Config());
        final ILock lock = instance.getLock("testIllegalConditionUsage");
        final ICondition condition = lock.newCondition("condition");
        try {
            condition.await();
        } catch (InterruptedException e) {
        }
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testIllegalConditionUsage2() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(new Config());
        final ILock lock = instance.getLock("testIllegalConditionUsage");
        final ICondition condition = lock.newCondition("condition");
        condition.signal();
    }

    @Test(timeout = 100000)
    public void testScheduledLockActionForDeadMember() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance(new Config());
        final ILock lock1 = h1.getLock("default");
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance(new Config());
        final ILock lock2 = h2.getLock("default");

        assertTrue(lock1.tryLock());

        final AtomicBoolean error = new AtomicBoolean(false);
        final Thread thread = new Thread(new Runnable() {
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
        h2.getLifecycleService().shutdown();
        thread.join(10000);
        assertFalse(thread.isAlive());
        assertFalse(error.get());

        assertTrue(lock1.isLocked());
        lock1.unlock();
        assertFalse(lock1.isLocked());
        assertTrue(lock1.tryLock());
    }

    @Test
    @Category(ProblematicTest.class)//TODO
    public void testLockInterruption() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "5000");
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);

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

    /**
     * Test for issue #39
     */
    @Test
    public void testIsLocked() throws InterruptedException {
        Config config = new Config();
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
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

    @Test(timeout = 1000 * 100)
    /**
     * Test for issue 267
     */
    public void testHighConcurrentLockAndUnlock() {
        Config config = new Config();
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
        final String key = "key";
        final int threadCount = 100;
        final int lockCountPerThread = 5000;
        final int locks = 50;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final AtomicInteger totalCount = new AtomicInteger();

        class InnerTest implements Runnable {
            public void run() {
                boolean live = true;
                Random rand = new Random();
                try {
                    for (int j = 0; j < lockCountPerThread && live; j++) {
                        final Lock lock = hz.getLock(key + rand.nextInt(locks));
                        lock.lock();
                        try {
                            totalCount.incrementAndGet();
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            break;
                        } finally {
                            try {
                                lock.unlock();
                            } catch (Exception e) {
                                e.printStackTrace();
                                live = false;
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }
        }

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(new InnerTest());
        }

        try {
            assertTrue("Lock tasks stuck!", latch.await(2, TimeUnit.MINUTES));
            assertEquals((threadCount * lockCountPerThread), totalCount.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                hz.getLifecycleService().terminate();
            } catch (Throwable ignored) {
            }
            executorService.shutdownNow();
        }
    }

}
