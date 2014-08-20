package com.hazelcast.concurrent.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class ConditionTest extends HazelcastTestSupport {

    @Test(expected = UnsupportedOperationException.class)
    public void testNewConditionWithoutNameIsNotSupported() {
        HazelcastInstance instance = createHazelcastInstance();

        ILock lock = instance.getLock(randomString());

        lock.newCondition();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testNewCondition_whenNullName() {
        HazelcastInstance instance = createHazelcastInstance();

        ILock lock = instance.getLock(randomString());

        lock.newCondition(null);
    }

    @Test(timeout = 60000)
    @Ignore
    public void testMultipleConditionsForSameLock() {

    }

    @Test(timeout = 60000)
    @Ignore
    public void testSameConditionRetrievedMultipleTimesForSameLock() {

    }

    @Test
    public void testAwaitTime_whenNotSignalled() throws InterruptedException {
        Config config = new Config();
        int callTimeoutMillis = 3000;
        config.setProperty("hazelcast.operation.call.timeout.millis", String.valueOf(callTimeoutMillis));
        HazelcastInstance instance = createHazelcastInstance(config);

        final ILock lock = instance.getLock(randomString());
        String name = randomString();
        final ICondition condition0 = lock.newCondition(name);

        final int awaitMillis = callTimeoutMillis * 3;

        lock.lock();
        try {
            final long begin = System.currentTimeMillis();
            condition0.await(awaitMillis, TimeUnit.MILLISECONDS);
            final long end = System.currentTimeMillis();
            assertEquals(awaitMillis, end - begin, 1000);
        } finally {
            lock.unlock();
        }
    }

    @Test(timeout = 60000)
    @Ignore
    public void testConditionsWithSameNameButDifferentLocksAreIndependent() {

    }

    @Test(timeout = 60000)
    public void testSignalWithSingleWaiter() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        String lockName = randomString();
        String conditionName = randomString();
        final ILock lock = instance.getLock(lockName);
        final ICondition condition = lock.newCondition(conditionName);
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

        assertEquals(false, lock.isLocked());
        lock.lock();
        assertEquals(true, lock.isLocked());
        condition.signal();
        lock.unlock();
        t.join();
        assertEquals(2, count.get());
    }

    @Test(timeout = 60000)
    public void testSignalAllWithSingleWaiter() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        String lockName = randomString();
        String conditionName = randomString();
        final ILock lock = instance.getLock(lockName);
        final ICondition condition = lock.newCondition(conditionName);
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
        lock.lock();
        condition.signalAll();
        lock.unlock();
        finalLatch.await(1, TimeUnit.MINUTES);
        assertEquals(k * 2, count.get());
    }

    @Test(timeout = 60000)
    @Ignore
    public void testInterruptionDuringWaiting() {

    }

    //if there are multiple waiters, then only 1 waiter should be notified.
    @Test(timeout = 60000)
    @Ignore
    public void testSignalWithMultipleWaiters() {
    }

    //a signal is send to wake up threads, but it isn't a flag set on the condition so that future waiters will
    //receive this signal
    @Test(timeout = 60000)
    @Ignore
    public void testSignalIsNotStored() {

    }

    @Test(timeout = 60000, expected = IllegalMonitorStateException.class)
    public void testAwaitOnConditionOfFreeLock() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        ICondition condition = lock.newCondition("condition");
        condition.await();
    }

    @Test(timeout = 60000, expected = IllegalMonitorStateException.class)
    public void testSignalOnConditionOfFreeLock() {
        HazelcastInstance instance = createHazelcastInstance();
        ILock lock = instance.getLock(randomString());
        ICondition condition = lock.newCondition("condition");
        condition.signal();
    }

    @Test(timeout = 60000)
    @Ignore
    public void testAwaitOnConditionOwnedByOtherThread() {

    }

    @Test(timeout = 60000)
    @Ignore
    public void testSignalOnConditionOwnedByOtherThread() {

    }

    @Test(timeout = 60000)
    @Ignore
    public void testAwait_whenTimeout() {

    }

    @Test(timeout = 60000)
    @Ignore
    public void testAwait_whenNegativeTimeout() {

    }

    @Test(timeout = 60000)
    @Ignore
    public void testAwait_nullTimeout() {

    }

    // ====================== tests to make sure the condition can deal with cluster member failure ====================

    @Test(timeout = 100000)
    public void testKeyOwnerDiesOnCondition() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final HazelcastInstance keyOwner = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();
        final AtomicInteger signalCounter = new AtomicInteger(0);

        final String key = generateKeyOwnedBy(instance1);
        final ILock lock1 = instance1.getLock(key);
        final String conditionName = randomString();
        final ICondition condition1 = lock1.newCondition(conditionName);

        Thread t = new Thread(new Runnable() {
            public void run() {
                ILock lock = instance2.getLock(key);
                ICondition condition = lock.newCondition(conditionName);
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

    @Test(timeout = 60000, expected = DistributedObjectDestroyedException.class)
    public void testDestroyLockWhenOtherWaitingOnConditionAwait() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        final ILock lock = instance.getLock(randomString());
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
        latch.countDown();
        condition.await();
        lock.unlock();
    }

    @Test(timeout = 60000, expected = HazelcastInstanceNotActiveException.class)
    public void testShutDownNodeWhenOtherWaitingOnConditionAwait() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        nodeFactory.newHazelcastInstance();
        final String name = randomString();
        final ILock lock = instance.getLock(name);
        final ICondition condition = lock.newCondition("condition");
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            public void run() {
                try {
                    latch.await(1, TimeUnit.MINUTES);
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                instance.shutdown();
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


    @Test
    @Category(ProblematicTest.class)
    public void testLockConditionSignalAllShutDownKeyOwner() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final String name = randomString();
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        final AtomicInteger count = new AtomicInteger(0);
        final int size = 50;
        final HazelcastInstance keyOwner = nodeFactory.newHazelcastInstance();
        warmUpPartitions(instance, keyOwner);

        final String key = generateKeyOwnedBy(keyOwner);

        final ILock lock = instance.getLock(key);
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

        ILock lock1 = keyOwner.getLock(key);
        ICondition condition1 = lock1.newCondition(name);
        awaitLatch.await(1, TimeUnit.MINUTES);
        lock1.lock();
        condition1.signalAll();
        lock1.unlock();
        keyOwner.shutdown();

        finalLatch.await(2, TimeUnit.MINUTES);
        assertEquals(size, count.get());
    }
}
