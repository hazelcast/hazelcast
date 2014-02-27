package com.hazelcast.client.lock;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientConditionTest extends HazelcastTestSupport{
    private static final String name = "test";
    private static HazelcastInstance client;
    private static ILock lock;
    private static HazelcastInstance hz;

    @BeforeClass
    public static void init() {
        hz = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
        lock = client.getLock(name);
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        lock.forceUnlock();
    }

    @Test
    public void testLockConditionSimpleUsage() throws InterruptedException {
        final String name = "testLockConditionSimpleUsage";
        final ILock lock = client.getLock(name);
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

        assertEquals(false, lock.isLocked());
        lock.lock();
        assertEquals(true, lock.isLocked());
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

        final ILock lock = client.getLock(k);
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
        keyOwner.shutdown();

        finalLatch.await(2, TimeUnit.MINUTES);
        assertEquals(size, count.get());
    }

    @Test(timeout = 100000)
    public void testKeyOwnerDiesOnCondition() throws Exception {
        final HazelcastInstance keyOwner = Hazelcast.newHazelcastInstance();

        final AtomicInteger signalCounter = new AtomicInteger(0);

        final String key = generateKeyOwnedBy(hz);
        final ILock lock1 = client.getLock(key);
        final String conditionName = randomString();
        final ICondition condition1 = lock1.newCondition(conditionName);

        Thread t = new Thread(new Runnable() {
            public void run() {
                final ILock lock = client.getLock(key);
                final ICondition condition = lock.newCondition(conditionName);
                lock.lock();
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
                signalCounter.incrementAndGet();
            }
        });
        t.start();
        Thread.sleep(1000);
        lock1.lock();
        keyOwner.shutdown();

        condition1.signal();

        lock1.unlock();
        Thread.sleep(1000);
        t.join();
        assertEquals(1, signalCounter.get());
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
}
