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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.Member;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.*;


/**
 * User: sancar
 * Date: 2/18/13
 * Time: 5:12 PM
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class LockTest {

    @Test
    public void testSimpleUsage() {
        // with multiple threads on single node
        // lock, tryLock, isLocked, unlock
    }

    @Test
    public void testSimpleUsageOnMultipleNodes() {
        // with multiple threads on multiple nodes
    }

    @Test(timeout = 100000)
    public void testLockOwnerDies() throws Exception {

    }

    @Test(timeout = 100000)
    public void testKeyOwnerDies() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        int key = 0;
        final Member expectedOwner = h2.getCluster().getLocalMember();
        while (h1.getPartitionService().getPartition(key++).getOwner().equals(expectedOwner));


    }

    @Test(timeout = 100000)
    public void testScheduledLockActionForDeadMember() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ILock lock1 = h1.getLock("default");
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final ILock lock2 = h2.getLock("default");
        assertTrue(lock1.tryLock());
        new Thread(new Runnable() {
            public void run() {
                try {
                    lock2.lock();
                    fail("Shouldn't be able to lock!");
                } catch (Throwable e) {
                }
            }
        }).start();
        Thread.sleep(2000);
        h2.getLifecycleService().shutdown();
        Thread.sleep(2000);
        lock1.unlock();
        assertTrue(lock1.tryLock());
    }

    @Test
    public void testLockInterruption() throws InterruptedException {
        Config config = new Config();
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        final Lock lock = hz.getLock("test");
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
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        final Lock lock = hz.getLock("test");
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    lock.tryLock(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    System.err.println(e);
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
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
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
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
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
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
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
            assertTrue("Lock tasks stuck!", latch.await(60, TimeUnit.SECONDS));
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
