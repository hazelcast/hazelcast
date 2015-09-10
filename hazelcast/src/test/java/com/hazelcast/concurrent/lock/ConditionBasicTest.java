package com.hazelcast.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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


public abstract class ConditionBasicTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    private HazelcastInstance callerInstance;

    @Before
    public void setup() {
        instances = newInstances();
        callerInstance = instances[0];
    }

    protected String newName() {
        HazelcastInstance target = instances[instances.length - 1];
        return generateKeyOwnedBy(target);
    }

    protected abstract HazelcastInstance[] newInstances();

    @Test(expected = UnsupportedOperationException.class)
    public void testNewConditionWithoutNameIsNotSupported() {
        ILock lock = callerInstance.getLock(newName());

        lock.newCondition();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testNewCondition_whenNullName() {
        ILock lock = callerInstance.getLock(newName());

        lock.newCondition(null);
    }

    @Test
    public void testAwaitNanos_remainingTime() throws InterruptedException {
        String name = newName();
        ILock lock = callerInstance.getLock(name);
        ICondition condition = lock.newCondition(name);

        lock.lock();
        long timeout = 1000L;
        long remainingTimeout = condition.awaitNanos(timeout);
        assertTrue("Remaining timeout should be <= 0, but it's = " + remainingTimeout,
                remainingTimeout <= 0);
    }

    @Test(timeout = 60000)
    public void testMultipleConditionsForSameLock() throws InterruptedException {
        ILock lock = callerInstance.getLock(newName());
        ICondition condition0 = lock.newCondition(newName());
        ICondition condition1 = lock.newCondition(newName());

        CountDownLatch latch = new CountDownLatch(2);
        CountDownLatch syncLatch = new CountDownLatch(2);

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
        ILock lock = callerInstance.getLock(newName());
        ICondition condition = lock.newCondition(newName());

        CountDownLatch latch = new CountDownLatch(2);
        CountDownLatch syncLatch = new CountDownLatch(2);

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
        ILock lock = callerInstance.getLock(newName());
        ICondition condition0 = lock.newCondition(newName());
        ICondition condition1 = lock.newCondition(newName());

        final CountDownLatch latch = new CountDownLatch(10);
        CountDownLatch syncLatch = new CountDownLatch(2);

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
        final ILock lock = callerInstance.getLock(newName());
        String name = newName();
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
        final ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());
        lock.lock();

        boolean success = condition.await(1, TimeUnit.MILLISECONDS);

        assertFalse(success);
        assertTrue(lock.isLockedByCurrentThread());
    }

    @Test(timeout = 60000)
    public void testConditionsWithSameNameButDifferentLocksAreIndependent() throws InterruptedException {
        String name = newName();
        ILock lock0 = callerInstance.getLock(newName());
        final ICondition condition0 = lock0.newCondition(name);
        ILock lock1 = callerInstance.getLock(newName());
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
        String lockName = newName();
        String conditionName = newName();
        final ILock lock = callerInstance.getLock(lockName);
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
        String lockName = newName();
        String conditionName = newName();
        final ILock lock = callerInstance.getLock(lockName);
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
        String lockName = newName();
        String conditionName = newName();
        final ILock lock = callerInstance.getLock(lockName);
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

    //if there are multiple waiters, then only 1 waiter should be notified.
    @Test(timeout = 60000)
    public void testSignalWithMultipleWaiters() throws InterruptedException {
        ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());

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
        ILock lock = callerInstance.getLock(newName());
        ICondition condition = lock.newCondition(newName());

        CountDownLatch latch = new CountDownLatch(1);

        lock.lock();
        condition.signal();
        lock.unlock();

        createThreadWaitsForCondition(latch, lock, condition, new CountDownLatch(0)).start();
        // if the thread is still waiting, then the signal is not stored.
        assertFalse(latch.await(3000, TimeUnit.MILLISECONDS));
    }

    @Test(timeout = 60000, expected = IllegalMonitorStateException.class)
    public void testAwaitOnConditionOfFreeLock() throws InterruptedException {
        ILock lock = callerInstance.getLock(newName());
        ICondition condition = lock.newCondition("condition");
        condition.await();
    }

    @Test(timeout = 60000, expected = IllegalMonitorStateException.class)
    public void testSignalOnConditionOfFreeLock() {
        ILock lock = callerInstance.getLock(newName());
        ICondition condition = lock.newCondition("condition");
        condition.signal();
    }

    @Test(timeout = 60000, expected = IllegalMonitorStateException.class)
    public void testAwait_whenOwnedByOtherThread() throws InterruptedException {
        final ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());

        final CountDownLatch latch = new CountDownLatch(1);

        new TestThread() {
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
        final ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());

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
        final ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());

        lock.lock();

        assertFalse(condition.await(1, TimeUnit.MILLISECONDS));
    }

    @Test(timeout = 60000)
    public void testAwaitTimeout_whenSuccess() throws InterruptedException {
        final ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());
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
        final ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());

        lock.lock();

        Date date = new Date();
        date.setTime(System.currentTimeMillis() + 1000);
        assertFalse(condition.awaitUntil(date));
    }

    @Test(timeout = 60000)
    public void testAwaitUntil_whenSuccess() throws InterruptedException {
        final ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
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
        final ILock lock = callerInstance.getLock(newName());
        final ICondition condition = lock.newCondition(newName());

        lock.lock();

        assertFalse(condition.await(-1, TimeUnit.MILLISECONDS));
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
