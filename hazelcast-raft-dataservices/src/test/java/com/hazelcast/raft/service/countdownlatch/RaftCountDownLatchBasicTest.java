package com.hazelcast.raft.service.countdownlatch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftCountDownLatchBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private ICountDownLatch latch;

    @Before
    public void setup() {
        instances = createInstances();
        latch = createLatch("latch");
        assertNotNull(latch);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(3);
    }

    protected ICountDownLatch createLatch(String name) {
        return create(instances[RandomPicker.getInt(instances.length)], RaftCountDownLatchService.SERVICE_NAME, name);
    }


    // ================= trySetCount =================================================

    @Test(expected = IllegalArgumentException.class)
    public void testTrySetCount_whenArgumentNegative() {
        latch.trySetCount(-20);
    }

    @Test
    public void testTrySetCount_whenCountIsZero() {
        assertTrue(latch.trySetCount(40));

        assertEquals(40, latch.getCount());
    }

    @Test
    public void testTrySetCount_whenCountIsNotZero() {
        latch.trySetCount(10);

        assertFalse(latch.trySetCount(20));
        assertFalse(latch.trySetCount(0));
        assertEquals(10, latch.getCount());
    }

    @Test
    public void testTrySetCount_whenPositive() {
        latch.trySetCount(10);

        assertFalse(latch.trySetCount(20));
        assertEquals(10, latch.getCount());
    }

    @Test
    public void testTrySetCount_whenAlreadySet() {
        latch.trySetCount(10);

        assertFalse(latch.trySetCount(20));
        assertFalse(latch.trySetCount(100));
        assertFalse(latch.trySetCount(0));
        assertEquals(10, latch.getCount());
    }

    // ================= countDown =================================================

    @Test
    public void testCountDown() {
        latch.trySetCount(20);

        for (int i = 19; i >= 0; i--) {
            latch.countDown();
            assertEquals(i, latch.getCount());
        }

        latch.countDown();
        assertEquals(0, latch.getCount());
    }

    // ================= getCount =================================================

    @Test
    public void testGetCount() {
        latch.trySetCount(20);
        assertEquals(20, latch.getCount());
    }

    // ================= destroy =================================================


    // ================= await =================================================

    @Test(expected = NullPointerException.class)
    public void testAwait_whenNullUnit() throws InterruptedException {
        latch.await(1, null);
    }

    @Test
    public void testAwait() {
        assertTrue(latch.trySetCount(1));
        spawn(new Runnable() {
            @Override
            public void run() {
                latch.countDown();
            }
        });
        assertOpenEventually(latch);
    }

    @Test
    public void testAwait_withManyThreads() {
        final CountDownLatch completedLatch = new CountDownLatch(10);

        latch.trySetCount(1);
        for (int i = 0; i < 10; i++) {
            new TestThread() {
                public void doRun() throws Exception {
                    if (latch.await(1, TimeUnit.MINUTES)) {
                        completedLatch.countDown();
                    }
                }
            }.start();
        }
        latch.countDown();
        assertOpenEventually(completedLatch);
    }

    @Test
    public void testAwait_whenTimeOut() throws InterruptedException {
        latch.trySetCount(1);
        long time = System.currentTimeMillis();
        assertFalse(latch.await(100, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - time;
        assertTrue(elapsed >= 100);
        assertEquals(1, latch.getCount());
    }

}
