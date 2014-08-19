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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @author mdogan 1/16/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CountDownLatchTest extends HazelcastTestSupport {

    @Test (expected = IllegalArgumentException.class)
    public void testTrySetCount_whenArgumentNegative() {
        final HazelcastInstance instance = createHazelcastInstance();
        ICountDownLatch latch = instance.getCountDownLatch("latch");
        latch.trySetCount(-20);
    }

    @Test
    @ClientCompatibleTest
    public void testSimpleUsage() throws InterruptedException {
        final int k = 5;
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
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

    @Test
    @ClientCompatibleTest
    public void testAwaitFail() throws InterruptedException {
        final int k = 3;
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
        ICountDownLatch latch = instances[0].getCountDownLatch("test");
        latch.trySetCount(k - 1);

        long t = System.currentTimeMillis();
        assertFalse(latch.await(100, TimeUnit.MILLISECONDS));
        final long elapsed = System.currentTimeMillis() - t;
        assertTrue(elapsed >= 100);
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    @ClientCompatibleTest
    public void testLatchDestroyed() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        final ICountDownLatch latch = hz1.getCountDownLatch("test");
        latch.trySetCount(2);

        new Thread() {
            public void run() {
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
                latch.destroy();
            }
        }.start();

        hz2.getCountDownLatch("test").await(5, TimeUnit.SECONDS);
    }

    @Test
    @ClientCompatibleTest
    public void testLatchMigration() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(5);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz2, hz1);

        ICountDownLatch latch1 = hz1.getCountDownLatch("test");
        latch1.trySetCount(10);
        Thread.sleep(500);

        ICountDownLatch latch2 = hz2.getCountDownLatch("test");
        assertEquals(10, latch2.getCount());
        latch2.countDown();
        assertEquals(9, latch1.getCount());
        hz1.shutdown();
        assertEquals(9, latch2.getCount());

        HazelcastInstance hz3 = factory.newHazelcastInstance();
        warmUpPartitions(hz3);
        ICountDownLatch latch3 = hz3.getCountDownLatch("test");
        latch3.countDown();
        assertEquals(8, latch3.getCount());

        hz2.shutdown();
        latch3.countDown();
        assertEquals(7, latch3.getCount());

        HazelcastInstance hz4 = factory.newHazelcastInstance();
        HazelcastInstance hz5 = factory.newHazelcastInstance();
        warmUpPartitions(hz5, hz4);
        Thread.sleep(250);

        hz3.shutdown();
        ICountDownLatch latch4 = hz4.getCountDownLatch("test");
        assertEquals(7, latch4.getCount());

        ICountDownLatch latch5 = hz5.getCountDownLatch("test");
        latch5.countDown();
        assertEquals(6, latch5.getCount());
        latch5.countDown();
        assertEquals(5, latch4.getCount());
        assertEquals(5, latch5.getCount());
    }
}
