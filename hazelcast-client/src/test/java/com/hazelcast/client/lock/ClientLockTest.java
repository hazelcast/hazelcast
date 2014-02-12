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

package com.hazelcast.client.lock;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author ali 5/28/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientLockTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static ILock l;

    @BeforeClass
    public static void init() {
        Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient(null);
        l = hz.getLock(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        l.forceUnlock();
    }

    @Test
    public void testLock() throws Exception {
        l.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (!l.tryLock()) {
                    latch.countDown();
                }
            }
        }.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        l.forceUnlock();
    }

    @Test
    public void testLockTtl() throws Exception {
        l.lock( 3, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread() {
            public void run() {
                if (!l.tryLock()) {
                    latch.countDown();
                }
                try {
                    if (l.tryLock( 5, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        l.forceUnlock();
    }

    @Test
    public void testTryLock() throws Exception {

        assertTrue(l.tryLock( 2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(!l.tryLock( 2, TimeUnit.SECONDS)){
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertTrue(l.isLocked());

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(l.tryLock( 20, TimeUnit.SECONDS)){
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);
        l.unlock();
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(l.isLocked());
        l.forceUnlock();
    }

    @Test
    public void testForceUnlock() throws Exception {
        l.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                l.forceUnlock();
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertFalse(l.isLocked());
    }

    @Test
    public void testLockConditionSimpleUsage() throws InterruptedException {
        final String name = "testLockConditionSimpleUsage";
        final ILock lock = hz.getLock(name);
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

        final ILock lock1 = hz.getLock(name);
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
        final String name = "testLockConditionSimpleUsage";
        final ILock lock = hz.getLock(name);
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
        final ILock lock1 = hz.getLock(name);
        final ICondition condition1 = lock1.newCondition(name + "c");
        lock1.lock();
        condition1.signalAll();
        lock1.unlock();
        finalLatch.await(1, TimeUnit.MINUTES);
        Assert.assertEquals(k * 2, count.get());
    }
    @Test
    public void testLockConditionSignalAllShutDownKeyOwner() throws InterruptedException {
        final String name = "testLockConditionSignalAllShutDownKeyOwner";
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final AtomicInteger count = new AtomicInteger(0);
        final int size = 50;
        int k = 0;
        final HazelcastInstance keyOwner = Hazelcast.newHazelcastInstance();
        while (!keyOwner.getCluster().getLocalMember().equals(instance.getPartitionService().getPartition(++k).getOwner())) {
            Thread.sleep(10);
        }

        final ILock lock = hz.getLock(k);
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

        final ILock lock1 = hz.getLock(k);
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
        final HazelcastInstance keyOwner = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        int k = 0;
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        while (keyOwner.getCluster().getLocalMember().equals(instance1.getPartitionService().getPartition(k++).getOwner())) {
            Thread.sleep(10);
        }

        final int key = k;
        final ILock lock1 = hz.getLock(key);
        final String name = "testKeyOwnerDiesOnCondition";
        final ICondition condition1 = lock1.newCondition(name);

        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = hz.getLock(key);
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
        final ILock lock = hz.getLock("testDestroyLockWhenOtherWaitingOnConditionAwait");
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
    public void testIllegalConditionUsage1() {
        final ICondition condition = l.newCondition("condition");
        try {
            condition.await();
        } catch (InterruptedException e) {
        }
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testIllegalConditionUsage2() {
        final ICondition condition = l.newCondition("condition");
        condition.signal();
    }
    @Test
    public void testConditionUsage() {
        l.lock();
        final ICondition condition = l.newCondition("condition");
        try {
            condition.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        l.unlock();
    }
    @Test
    public void testStats() throws InterruptedException {
        l.lock();
        assertTrue(l.isLocked());
        assertTrue(l.isLockedByCurrentThread());
        assertEquals(1, l.getLockCount());

        l.unlock();
        assertFalse(l.isLocked());
        assertEquals(0, l.getLockCount());
        assertEquals(-1L, l.getRemainingLeaseTime());

        l.lock(1, TimeUnit.MINUTES);
        assertTrue(l.isLocked());
        assertTrue(l.isLockedByCurrentThread());
        assertEquals(1, l.getLockCount());
        assertTrue(l.getRemainingLeaseTime() > 1000 * 30);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                assertTrue(l.isLocked());
                assertFalse(l.isLockedByCurrentThread());
                assertEquals(1, l.getLockCount());
                assertTrue(l.getRemainingLeaseTime() > 1000 * 30);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }
}
