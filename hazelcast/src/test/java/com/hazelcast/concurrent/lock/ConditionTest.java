package com.hazelcast.concurrent.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    public void testMultipleConditionsForSameLock() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition0 = lock.newCondition(randomString());
        final ICondition condition1 = lock.newCondition(randomString());

        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch syncLatch = new CountDownLatch(2);

        createThreadWaitsForCondition(latch, lock, condition0, syncLatch).start();
        createThreadWaitsForCondition(latch, lock, condition1, syncLatch).start();

        syncLatch.await();

        lock.lock();
        condition0.signal();
        lock.unlock();

        lock.lock();
        condition1.signal();
        lock.unlock();

        assertOpenEventually(latch);
    }

    @Test(timeout = 60000)
    public void testSignalAll() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());

        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch syncLatch = new CountDownLatch(2);

        createThreadWaitsForCondition(latch, lock, condition, syncLatch).start();
        createThreadWaitsForCondition(latch, lock, condition, syncLatch).start();

        syncLatch.await();

        lock.lock();
        condition.signalAll();
        lock.unlock();

        assertOpenEventually(latch);
    }

    @Test(timeout = 60000)
    public void testSignalAll_whenMultipleConditions() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition0 = lock.newCondition(randomString());
        final ICondition condition1 = lock.newCondition(randomString());

        final CountDownLatch latch = new CountDownLatch(10);
        final CountDownLatch syncLatch = new CountDownLatch(2);

        createThreadWaitsForCondition(latch, lock, condition0, syncLatch).start();
        createThreadWaitsForCondition(latch, lock, condition1, syncLatch).start();

        syncLatch.await();

        lock.lock();
        condition0.signalAll();
        lock.unlock();

        assertTrueDelayed5sec(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(latch.getCount(), 9);
            }
        });
    }

    @Test(timeout = 60000)
    public void testSameConditionRetrievedMultipleTimesForSameLock() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        String name = randomString();
        final ICondition condition0 = lock.newCondition(name);
        final ICondition condition1 = lock.newCondition(name);

        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch syncLatch = new CountDownLatch(2);

        createThreadWaitsForCondition(latch, lock, condition0, syncLatch).start();
        createThreadWaitsForCondition(latch, lock, condition1, syncLatch).start();

        syncLatch.await();

        lock.lock();
        condition0.signalAll();
        lock.unlock();

        assertOpenEventually(latch);
    }

    @Test
    public void testAwaitTime_whenTimeout() throws InterruptedException {
       HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());
        lock.lock();

        boolean success = condition.await(1, TimeUnit.MILLISECONDS);

        assertFalse(success);
        assertTrue(lock.isLockedByCurrentThread());
    }

    @Test(timeout = 60000)
    public void testConditionsWithSameNameButDifferentLocksAreIndependent() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        String name = randomString();
        ILock lock0 = instance.getLock(randomString());
        final ICondition condition0 = lock0.newCondition(name);
        ILock lock1 = instance.getLock(randomString());
        final ICondition condition1 = lock1.newCondition(name);

        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch syncLatch = new CountDownLatch(2);

        createThreadWaitsForCondition(latch, lock0, condition0, syncLatch).start();

        createThreadWaitsForCondition(latch, lock1, condition1, syncLatch).start();

        syncLatch.await();

        lock0.lock();
        condition0.signalAll();
        lock0.unlock();

        lock1.lock();
        condition1.signalAll();
        lock1.unlock();

        assertOpenEventually(latch);
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

    /**
     * Testcase for #3025. Tests that an await with short duration in a highly-contended lock does not generate an
     * IllegalStateException (previously due to a race condition in the waiter list for the condition).
     */
    @Test(timeout = 60000)
    public void testContendedLockUnlockWithVeryShortAwait() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        String lockName = randomString();
        String conditionName = randomString();
        final ILock lock = instance.getLock(lockName);
        final ICondition condition = lock.newCondition(conditionName);

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicReference<Exception> errorRef = new AtomicReference<Exception>();
        final int numberOfThreads = 8;
        final CountDownLatch finalLatch = new CountDownLatch(numberOfThreads);
        ExecutorService ex = Executors.newCachedThreadPool();

        for (int i = 0; i < numberOfThreads; i++) {
            ex.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (running.get()) {
                            lock.lock();
                            try {
                                condition.await(1, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException ignored) {
                            } catch (IllegalStateException e) {
                                errorRef.set(e);
                                running.set(false);
                            } finally {
                                lock.unlock();
                            }
                        }
                    } finally {
                        finalLatch.countDown();
                    }
                }
            });
        }

        ex.execute(new Runnable() {
            @Override
            public void run() {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
                running.set(false);
            }
        });

        try {
            finalLatch.await(30, TimeUnit.SECONDS);
            assertNull("await() on condition threw IllegalStateException!", errorRef.get());
        } finally {
            ex.shutdownNow();
        }
    }

    @Test(timeout = 60000)
    public void testInterruptionDuringWaiting() throws InterruptedException {
        Config config = new Config();
        // the system should wait at most 5000 ms in order to determine the operation status
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "5000");

        HazelcastInstance instance = createHazelcastInstance(config);

        final ILock lock = instance.getLock(randomString());
        final ICondition condition0 = lock.newCondition(randomString());

        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.lock();
                    condition0.await();
                } catch (InterruptedException e) {
                    latch.countDown();
                }
            }
        });
        thread.start();

        sleepSeconds(2);
        thread.interrupt();

        assertOpenEventually(latch);
    }

    //if there are multiple waiters, then only 1 waiter should be notified.
    @Test(timeout = 60000)
    public void testSignalWithMultipleWaiters() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());

        final CountDownLatch latch = new CountDownLatch(10);
        final CountDownLatch syncLatch = new CountDownLatch(3);

        createThreadWaitsForCondition(latch, lock, condition, syncLatch).start();
        createThreadWaitsForCondition(latch, lock, condition, syncLatch).start();
        createThreadWaitsForCondition(latch, lock, condition, syncLatch).start();

        syncLatch.await();

        lock.lock();
        condition.signal();
        lock.unlock();

        assertTrueDelayed5sec(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(9, latch.getCount());
            }
        });

        assertEquals(false, lock.isLocked());
    }

    //a signal is send to wake up threads, but it isn't a flag set on the condition so that future waiters will
    //receive this signal
    @Test(timeout = 60000)
    public void testSignalIsNotStored() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());

        final CountDownLatch latch = new CountDownLatch(1);

        lock.lock();
        condition.signal();
        lock.unlock();

        createThreadWaitsForCondition(latch, lock, condition, null).start();
        // if the thread is still waiting, then the signal is not stored.
        assertFalse(latch.await(3000, TimeUnit.MILLISECONDS));
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

    @Test(timeout = 60000, expected = IllegalMonitorStateException.class)
    public void testAwait_whenOwnedByOtherThread() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());

        final CountDownLatch latch = new CountDownLatch(1);

        new TestThread(){
            @Override
            public void doRun() {
                lock.lock();
                latch.countDown();
            }
        }.start();

        latch.await();

        condition.await();
    }

    @Test(timeout = 60000, expected = IllegalMonitorStateException.class)
    public void testSignal_whenOwnedByOtherThread() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());

        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                latch.countDown();
            }
        }).start();

        latch.await();

        condition.signal();
    }

    @Test(timeout = 60000)
    public void testAwaitTimeout_whenFail() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());

        lock.lock();

        assertFalse(condition.await(1, TimeUnit.MILLISECONDS));
    }

    @Test(timeout = 60000)
    public void testAwaitTimeout_whenSuccess() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                try {
                    if (condition.await(10, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        sleepSeconds(1);
        lock.lock();
        condition.signal();
        lock.unlock();
    }

    @Test(timeout = 60000)
    public void testAwaitUntil_whenFail() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());

        lock.lock();

        Date date = new Date();
        date.setTime(System.currentTimeMillis() + 1000);
        assertFalse(condition.awaitUntil(date));
    }

    @Test(timeout = 60000)
    public void testAwaitUntil_whenSuccess() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                try {
                    Date date = new Date();
                    date.setTime(System.currentTimeMillis() + 10000);
                    if (condition.awaitUntil(date)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        sleepSeconds(1);
        lock.lock();
        condition.signal();
        lock.unlock();
    }

    // See #3262
    @Test(timeout = 60000)
    @Ignore
    public void testAwait_whenNegativeTimeout() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();

        final ILock lock = instance.getLock(randomString());
        final ICondition condition = lock.newCondition(randomString());

        lock.lock();

        assertFalse(condition.await(-1, TimeUnit.MILLISECONDS));
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
    public void testDestroyLock_whenOtherWaitingOnConditionAwait() throws InterruptedException {
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
    public void testShutDownNode_whenOtherWaitingOnConditionAwait() throws InterruptedException {
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

    private TestThread createThreadWaitsForCondition(final CountDownLatch latch, final ILock lock, final ICondition condition, final CountDownLatch syncLatch) {
        TestThread t = new TestThread() {
            public void doRun() throws Exception {
                try {
                    lock.lock();
                    syncLatch.countDown();
                    condition.await();
                    latch.countDown();
                } finally {
                    lock.unlock();
                }
            }
        };
        return t;
    }
}
