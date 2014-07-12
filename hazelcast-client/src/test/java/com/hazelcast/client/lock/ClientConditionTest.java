package com.hazelcast.client.lock;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientConditionTest extends HazelcastTestSupport {

    private static final String name = "test";

    private static HazelcastInstance client;
    private static ILock lock;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
        lock = client.getLock(name);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void reset() throws IOException {
        lock.forceUnlock();
    }

    @Test
    public void testLockConditionSimpleUsage() throws InterruptedException {
        String name = "testLockConditionSimpleUsage";
        final ILock lock = client.getLock(name);
        final ICondition condition = lock.newCondition(name + "c");
        final AtomicInteger count = new AtomicInteger(0);

        Thread thread = new Thread(new Runnable() {
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
        thread.start();
        Thread.sleep(1000);

        assertEquals(false, lock.isLocked());
        lock.lock();
        assertEquals(true, lock.isLocked());
        condition.signal();
        lock.unlock();
        thread.join();
        assertEquals(2, count.get());
    }

    @Test
    public void testLockConditionSignalAll() throws InterruptedException {
        String name = "testLockConditionSimpleUsage";
        final ILock lock = client.getLock(name);
        final ICondition condition = lock.newCondition(name + "c");
        final AtomicInteger count = new AtomicInteger(0);

        int threadCount = 50;
        final CountDownLatch awaitLatch = new CountDownLatch(threadCount);
        final CountDownLatch finalLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
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
        assertEquals(threadCount * 2, count.get());
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
        } catch (InterruptedException ignored) {
        }
        lock.unlock();
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testIllegalConditionUsageWithoutAcquiringLock() {
        ICondition condition = lock.newCondition("condition");
        try {
            condition.await();
        } catch (InterruptedException ignored) {
        }
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testIllegalConditionUsageSignalToNonAwaiter() {
        ICondition condition = lock.newCondition("condition");
        condition.signal();
    }

    @Test
    public void testConditionUsage() throws InterruptedException {
        lock.lock();
        ICondition condition = lock.newCondition("condition");
        condition.await(1, TimeUnit.SECONDS);
        lock.unlock();
    }
}
