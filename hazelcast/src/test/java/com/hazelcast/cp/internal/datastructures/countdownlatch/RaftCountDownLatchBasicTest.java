/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftCountDownLatchBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private ICountDownLatch latch;

    @Before
    public void setup() {
        instances = createInstances();
        latch = createLatch("latch@group1");
        assertNotNull(latch);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(3);
    }

    protected ICountDownLatch createLatch(String name) {
        HazelcastInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getCountDownLatch(name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProxyOnMetadataCPGroup() {
        instances[0].getCPSubsystem().getCountDownLatch("latch@metadata");
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
        spawn(() -> latch.countDown());
        assertOpenEventually(latch);
    }

    @Test
    public void testAwait_withManyThreads() {
        CountDownLatch completedLatch = new CountDownLatch(10);

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

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testCountDown_afterDestroy() {
        latch.destroy();

        latch.countDown();
    }

}
