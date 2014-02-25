package com.hazelcast.client.lock;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
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

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientConditionTest {
    static final String name = "test";
    static HazelcastInstance hz;
    static ILock l;

    @BeforeClass
    public static void init() {
        Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient();
        l = hz.getLock(name);
    }

    @AfterClass
    public static void destroy() {
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        l.forceUnlock();
    }

    @Test
    public void testLockConditionSimpleUsage() throws InterruptedException {
        final String name = "testLockConditionSimpleUsage";
        final ILock lock = hz.getLock(name);
        final ICondition condition = lock.newCondition(name + "c");
        final AtomicInteger count = new AtomicInteger(0);

        Thread t = new Thread(new Runnable() {
            public void run() {
                lock.lock();
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
        Thread.sleep(1000);

        Assert.assertEquals(false, lock.isLocked());
        lock.lock();
        Assert.assertEquals(true, lock.isLocked());
        condition.signal();
        lock.unlock();
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
        final Member keyOwnerMember = keyOwner.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        while (!keyOwnerMember.equals(partitionService.getPartition(++k).getOwner())) {
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

        awaitLatch.await(1, TimeUnit.MINUTES);
        lock.lock();
        condition.signalAll();
        lock.unlock();
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
        final Member keyOwnerMember = keyOwner.getCluster().getLocalMember();
        final PartitionService partitionService = instance1.getPartitionService();
        while (keyOwnerMember.equals(partitionService.getPartition(k++).getOwner())) {
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
    public void testIllegalConditionUsageWithoutAcquiringLock() {
        final ICondition condition = l.newCondition("condition");
        try {
            condition.await();
        } catch (InterruptedException e) {
        }
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testIllegalConditionUsageSignalToNonAwaiter() {
        final ICondition condition = l.newCondition("condition");
        condition.signal();
    }

    @Test
    public void testConditionUsage() throws InterruptedException {
        l.lock();
        final ICondition condition = l.newCondition("condition");
        condition.await(1, TimeUnit.SECONDS);
        l.unlock();
    }
}
