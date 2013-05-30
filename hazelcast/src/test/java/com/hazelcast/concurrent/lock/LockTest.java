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
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
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
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
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
        Thread.sleep(1000);
        Assert.assertEquals(true, lock.isLocked());
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

        final int key;
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
        final AtomicInteger integer = new AtomicInteger(0);
        final HazelcastInstance lockOwner = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final String name = "testLockOwnerDies";
        final ILock lock = lockOwner.getLock(name);
        lock.lock();
        Assert.assertEquals(true, lock.isLocked());
        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = instance1.getLock(name);
                lock.lock();
                integer.incrementAndGet();

            }
        });
        t.start();
        Assert.assertEquals(0, integer.get());
        lockOwner.getLifecycleService().shutdown();
        Thread.sleep(5000);
        Assert.assertEquals(1, integer.get());
    }

    @Test(timeout = 100000)
    public void testKeyOwnerDies() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final Config config = new Config();
        final HazelcastInstance keyOwner = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        int k = 0;
        final Member localMember = keyOwner.getCluster().getLocalMember();
        while (!localMember.equals(instance1.getPartitionService().getPartition(++k).getOwner())) {
            Thread.sleep(10);
        }

        final int key = k;
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

        keyOwner.getLifecycleService().shutdown();
        Assert.assertTrue(lock1.isLocked());
        Assert.assertTrue(lock1.tryLock());
        lock1.unlock();
        lock1.unlock();
        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
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

        final int key;
        if (localKey) {
            key = generateKeyOwnedBy(instance1);
        } else {
            key = generateKeyNotOwnedBy(instance1);
        }

        final ILock lock = instance1.getLock(key);
        lock.lock(3, TimeUnit.SECONDS);
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
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
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
                    if (lock.isLocked() && lock.tryLock()) {
                        count.incrementAndGet();
                        lock.unlock();
                    }
                    condition.await();
                    if (lock.isLocked() && lock.tryLock()) {
                        count.incrementAndGet();
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    return;
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
                        if (lock.isLocked() && lock.tryLock()) {
                            count.incrementAndGet();
                            lock.unlock();
                        }
                        awaitLatch.countDown();
                        condition.await();
                        if (lock.isLocked() && lock.tryLock()) {
                            count.incrementAndGet();
                            lock.unlock();
                        }
                    } catch (InterruptedException e) {
                        return;
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
                        if (lock.isLocked() && lock.tryLock()) {
                            count.incrementAndGet();
                            lock.unlock();
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

        new Thread(new Runnable() {
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
        try {
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

        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                instance.getLifecycleService().shutdown();
            }
        }).start();
        lock.lock();
        try {
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
    public void testLockInterruption() throws InterruptedException {
        Config config = new Config();
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);

        final Lock lock = hz.getLock("testLockInterruption");
        Random rand = new Random();
        for (int i = 0; i < 30; i++) {
            Thread t = new Thread() {
                public void run() {
                    try {
                        lock.lock();
                        sleep(1);
                    } catch (InterruptedException e) {
                    } finally {
                        lock.unlock();
                    }
                }
            };

            t.start();
            Thread.sleep(rand.nextInt(3));
            t.interrupt();
            t.join();

            if (!lock.tryLock(3, TimeUnit.SECONDS)) {
                fail("Could not acquire lock!");
            } else {
                lock.unlock();
            }
            Thread.sleep(100);
        }
    }

    @Test
    public void testLockInterruption2() throws InterruptedException {
        Config config = new Config();
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);

        final Lock lock = hz.getLock("testLockInterruption2");
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    lock.tryLock(60, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                } finally {
                    lock.unlock();
                }
            }
        });
        lock.lock();
        t.start();
        Thread.sleep(250);
        t.interrupt();
        Thread.sleep(1000);
        lock.unlock();
        Thread.sleep(500);
        assertTrue("Could not acquire lock!", lock.tryLock());
    }

    /**
     * Test for issue #39
     */
    @Test
    public void testLockIsLocked() throws InterruptedException {
        Config config = new Config();
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);;
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        final ILock lock = h1.getLock("testLockIsLocked");
        final ILock lock2 = h2.getLock("testLockIsLocked");

        assertFalse(lock.isLocked());
        assertFalse(lock2.isLocked());
        lock.lock();
        assertTrue(lock.isLocked());
        assertTrue(lock2.isLocked());

        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                ILock lock3 = h3.getLock("testLockIsLocked");
                assertTrue(lock3.isLocked());
                try {
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
        Thread.sleep(100);
        lock.unlock();
        assertTrue(latch.await(3, TimeUnit.SECONDS));
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
                hz.getLifecycleService().kill();
            } catch (Throwable ignored) {
            }
            executorService.shutdownNow();
        }
    }

}
