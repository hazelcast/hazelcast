/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class CountDownLatchAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected ICountDownLatch latch;

    @Before
    public void setup() {
        instances = newInstances();
        latch = newInstance();
    }

    protected ICountDownLatch newInstance() {
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        return local.getCountDownLatch(name);
    }

    protected abstract HazelcastInstance[] newInstances();

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

    @Test(timeout = 15000)
    public void testAwait() throws InterruptedException {
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
        latch.trySetCount(1);
        long time = System.currentTimeMillis();
        assertFalse(latch.await(100, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - time;
        assertTrue(elapsed >= 100);
        assertEquals(1, latch.getCount());
    }

    // ================= simple usage =================================================


    public static void assertOpenEventually(ICountDownLatch latch) {
        try {
            boolean completed = latch.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
            assertTrue(format("CountDownLatch failed to complete within %d seconds, count left: %d", ASSERT_TRUE_EVENTUALLY_TIMEOUT,
                    latch.getCount()), completed);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
