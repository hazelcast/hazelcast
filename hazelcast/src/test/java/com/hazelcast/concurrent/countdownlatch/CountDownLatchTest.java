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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CountDownLatchTest extends HazelcastTestSupport {

    // ================= trySetCount =================================================

    @Test(expected = IllegalArgumentException.class)
    public void testTrySetCount_whenArgumentNegative() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch("latch");
        latch.trySetCount(-20);
    }

    @Test
    public void testTrySetCount_whenCountIsZero() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());

        assertTrue(latch.trySetCount(40));
        assertEquals(40, latch.getCount());
    }

    @Test
    public void testTrySetCount_whenCountIsNotZero() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());
        latch.trySetCount(10);
        assertFalse(latch.trySetCount(20));
        assertFalse(latch.trySetCount(0));
        assertEquals(10, latch.getCount());
    }

    @Test
    public void testTrySetCount_whenPositive() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());

        latch.trySetCount(10);
        assertFalse(latch.trySetCount(20));
        assertEquals(10, latch.getCount());
    }

    @Test
    public void testTrySetCount_whenAlreadySet() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());

        latch.trySetCount(10);
        assertFalse(latch.trySetCount(20));
        assertFalse(latch.trySetCount(100));
        assertFalse(latch.trySetCount(0));
        assertEquals(10, latch.getCount());
    }

    // ================= countDown =================================================

    @Test
    public void testCountDown() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());

        latch.trySetCount(20);
        for (int i = 19; i >= 0; i--) {
            latch.countDown();
            assertEquals(i, latch.getCount());
        }
    }

    @Test
    public void testCountDown_whenReachZero_thenLatchRemoved() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());
        CountDownLatchService service = getNode(instance).getNodeEngine().getService(CountDownLatchService.SERVICE_NAME);

        latch.trySetCount(1);
        assertTrue(service.containsLatch(latch.getName()));
        latch.countDown();
        assertFalse(service.containsLatch(latch.getName()));
    }

    // ================= getCount =================================================

    @Test
    public void testGetCount() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());

        latch.trySetCount(20);
        assertEquals(20, latch.getCount());
    }

    // ================= destroy =================================================

    @Test
    public void testDestroy() {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());
        NodeEngineImpl nodeEngine = getNode(instance).getNodeEngine();
        CountDownLatchService service = nodeEngine.getService(CountDownLatchService.SERVICE_NAME);

        latch.destroy();
        assertFalse(service.containsLatch(latch.getName()));
    }

    // ================= await =================================================
    @Test(timeout = 15000)
    public void testAwait() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        final ICountDownLatch latch = instance.getCountDownLatch(randomString());
        latch.trySetCount(1);
        TestThread thread = new TestThread() {
            public void doRun() {
                latch.countDown();
            }
        };
        thread.start();
        assertOpenEventually(latch);
    }

    @Test(timeout = 15000)
    public void testAwait_withManyThreads() {
        HazelcastInstance instance = createHazelcastInstance();
        final ICountDownLatch latch = instance.getCountDownLatch(randomString());
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

    @Test(timeout = 15000)
    public void testAwait_whenTimeOut() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch(randomString());

        latch.trySetCount(1);
        long time = System.currentTimeMillis();
        assertFalse(latch.await(100, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - time;
        assertTrue(elapsed >= 100);
        assertEquals(1, latch.getCount());
    }

    @Test
    public void testAwait_whenInstanceShutdown_thenLatchOpened() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        final ICountDownLatch latch = instance.getCountDownLatch(randomString());
        latch.trySetCount(10);

        final TestThread awaitThread = new TestThread() {
            @Override
            public void doRun() throws Exception {
                latch.await(1, TimeUnit.HOURS);
            }
        };
        awaitThread.start();

        // give the awaitthread some time to get in the waiting state
        sleepSeconds(5);
        instance.shutdown();
        awaitThread.assertFailsEventually(HazelcastInstanceNotActiveException.class);
    }
    // ================= simple usage =================================================

    @Test
    @ClientCompatibleTest
    public void testSimpleUsage() throws InterruptedException {
        final int k = 5;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances();
        ICountDownLatch latch = instances[0].getCountDownLatch("test");
        latch.trySetCount(k - 1);
        assertEquals(k - 1, latch.getCount());

        new Thread() {
            public void run() {
                for (int i = 1; i < k; i++) {
                    try {
                        sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    final ICountDownLatch l = instances[i].getCountDownLatch("test");
                    l.countDown();
                    assertEquals(k - 1 - i, l.getCount());
                }
            }
        }.start();

        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
        assertEquals(0, latch.getCount());
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    @ClientCompatibleTest
    public void testLatchDestroyed() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        final ICountDownLatch latch = hz1.getCountDownLatch("test");
        latch.trySetCount(2);

        new TestThread() {
            public void doRun() throws Exception {
                sleep(1000);
                latch.destroy();
            }
        }.start();

        hz2.getCountDownLatch("test").await(5, TimeUnit.SECONDS);
    }

    public static void assertOpenEventually(ICountDownLatch latch) {
        try {
            boolean completed = latch.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
            assertTrue(format("CountDownLatch failed to complete within %d seconds , count left: %d", ASSERT_TRUE_EVENTUALLY_TIMEOUT,
                    latch.getCount()), completed);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
