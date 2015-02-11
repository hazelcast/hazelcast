package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationTimeoutTest extends HazelcastTestSupport {

    @Test
    public void testInterruptionDuringBlockingOp1() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        final IQueue<Object> q = hz.getQueue("queue");

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);

        OpThread thread = new OpThread("Queue Thread", latch, interruptedFlag) {
            protected void doOp() throws InterruptedException {
                q.poll(1, TimeUnit.MINUTES);
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        q.offer("new item!");

        assertTrue(latch.await(1, TimeUnit.MINUTES));

        if (thread.isInterruptionCaught()) {
            assertFalse("Thread interrupted flag should not be set!", interruptedFlag.get());
            assertFalse("Queue should not be empty!", q.isEmpty());
        } else {
            assertTrue("Thread interrupted flag should be set!", interruptedFlag.get());
            assertTrue("Queue should be empty!", q.isEmpty());
        }
    }

    @Test
    public void testWaitingIndefinitely() throws InterruptedException {
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "3000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);

        // need to warm-up partitions,
        // since waiting for lock backup can take up to 5 seconds
        // and that may cause OperationTimeoutException with "No response for 4000 ms" error.
        warmUpPartitions(instances);

        final String name = "testWaitingIndefinitely";
        ILock lock = instances[0].getLock(name);
        lock.lock();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    // because max timeout=3000 we get timeout exception which we should not
                    instances[1].getLock(name).lock();
                    latch.countDown();
                } catch (Exception ignored) {
                }
            }
        }.start();

        // wait for enough time which is greater than max-timeout (3000)
        sleepSeconds(10);
        lock.unlock();

        assertTrue(latch.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void testWaitingInfinitelyForTryLock() throws InterruptedException {
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "3000");
        final HazelcastInstance hz = createHazelcastInstance(config);
        final CountDownLatch latch = new CountDownLatch(1);

        final String name = "testWaitingInfinitelyForTryLock";
        hz.getLock(name).lock();

        new Thread() {
            public void run() {
                try {
                    hz.getLock(name).tryLock(10, TimeUnit.SECONDS);
                    latch.countDown();
                } catch (Exception ignored) {
                }
            }
        }.start();

        assertTrue(latch.await(20, TimeUnit.SECONDS));

    }

    @Test
    public void testInterruptionDuringBlockingOp2() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        final ILock lock = hz.getLock("lock");
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);

        final OpThread thread = new OpThread("Lock-Thread", latch, interruptedFlag) {
            protected void doOp() throws InterruptedException {
                assertTrue(lock.tryLock(1, TimeUnit.MINUTES));
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        lock.unlock();

        assertTrue(latch.await(1, TimeUnit.MINUTES));

        if (thread.isInterruptionCaught()) {
            assertFalse("Thread interrupted flag should not be set!", interruptedFlag.get());
            assertFalse("Lock should not be in 'locked' state!", lock.isLocked());
        } else {
            assertTrue("Thread interrupted flag should be set! " + thread, interruptedFlag.get());
            assertTrue("Lock should be 'locked' state!", lock.isLocked());
        }
    }

    private abstract class OpThread extends Thread {
        final CountDownLatch latch;
        final AtomicBoolean interruptionCaught = new AtomicBoolean(false);
        final AtomicBoolean interruptedFlag;

        protected OpThread(String name, CountDownLatch latch, AtomicBoolean interruptedFlag) {
            super(name);
            this.latch = latch;
            this.interruptedFlag = interruptedFlag;
        }

        public void run() {
            try {
                doOp();
                interruptedFlag.set(isInterrupted());
            } catch (InterruptedException e) {
                interruptionCaught.set(true);
            } finally {
                latch.countDown();
            }
        }

        private boolean isInterruptionCaught() {
            return interruptionCaught.get();
        }

        protected abstract void doOp() throws InterruptedException;
    }

}
