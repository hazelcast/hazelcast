package com.hazelcast.client.lock;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientConcurrentLockTest {

    private static HazelcastInstance server;
    private static HazelcastInstance client;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        server.shutdown();
        client.shutdown();
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test
    public void concurrent_TryLockTest() throws InterruptedException {
        concurrent_LockTest(false);
    }

    @Test
    public void concurrent_TryLock_WithTimeOutTest() throws InterruptedException {
        concurrent_LockTest(true);
    }

    private void concurrent_LockTest(boolean tryLockWithTimeOut) throws InterruptedException {
        ILock lock = client.getLock(randomString());
        AtomicInteger upTotal = new AtomicInteger(0);
        AtomicInteger downTotal = new AtomicInteger(0);

        LockTestThread threads[] = new LockTestThread[8];
        for (int i = 0; i < threads.length; i++) {
            LockTestThread lockTestThread;

            if (tryLockWithTimeOut) {
                lockTestThread = new TryLockWithTimeOutThread(lock, upTotal, downTotal);
            } else {
                lockTestThread = new TryLockThread(lock, upTotal, downTotal);
            }
            lockTestThread.start();
            threads[i] = lockTestThread;
        }

        assertJoinable(threads);
        assertEquals("concurrent access to locked code caused wrong total", 0, upTotal.get() + downTotal.get());
    }

    static class TryLockThread extends LockTestThread {
        public TryLockThread(ILock lock, AtomicInteger upTotal, AtomicInteger downTotal) {
            super(lock, upTotal, downTotal);
        }

        public void doRun() throws Exception {
            if (lock.tryLock()) {
                work();
                lock.unlock();
            }
        }
    }

    static class TryLockWithTimeOutThread extends LockTestThread {
        public TryLockWithTimeOutThread(ILock lock, AtomicInteger upTotal, AtomicInteger downTotal) {
            super(lock, upTotal, downTotal);
        }

        public void doRun() throws Exception {
            if (lock.tryLock(1, TimeUnit.MILLISECONDS)) {
                work();
                lock.unlock();
            }
        }
    }

    static abstract class LockTestThread extends Thread {
        private static final int ITERATIONS = 1000 * 10;
        private final Random random = new Random();
        protected final ILock lock;
        protected final AtomicInteger upTotal;
        protected final AtomicInteger downTotal;

        public LockTestThread(ILock lock, AtomicInteger upTotal, AtomicInteger downTotal) {
            this.lock = lock;
            this.upTotal = upTotal;
            this.downTotal = downTotal;
        }

        public void run() {
            try {
                for (int i = 0; i < ITERATIONS; i++) {
                    doRun();
                }
            } catch (Exception e) {
                throw new RuntimeException("LockTestThread throws: ", e);
            }
        }

        abstract void doRun() throws Exception;

        protected void work() {
            int delta = random.nextInt(1000);
            upTotal.addAndGet(delta);
            downTotal.addAndGet(-delta);
        }
    }
}
