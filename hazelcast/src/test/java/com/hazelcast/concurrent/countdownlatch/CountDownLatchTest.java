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
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

/**
 * @mdogan 1/16/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class CountDownLatchTest extends HazelcastTestSupport {

    @Test
    @ClientCompatibleTest
    public void testSimpleUsage() {
        final int k = 5;
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
        ICountDownLatch latch = instances[0].getCountDownLatch("test");
        latch.trySetCount(k - 1);
        Assert.assertEquals(k - 1, latch.getCount());

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
                    Assert.assertEquals(k - 1 - i, l.getCount());
                }
            }
        }.start();

        try {
            Assert.assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(0, latch.getCount());
    }

    @Test
    @ClientCompatibleTest
    public void testAwaitFail() {
        final int k = 3;
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
        ICountDownLatch latch = instances[0].getCountDownLatch("test");
        latch.trySetCount(k - 1);

        try {
            long t = System.currentTimeMillis();
            Assert.assertFalse(latch.await(100, TimeUnit.MILLISECONDS));
            final long elapsed = System.currentTimeMillis() - t;
            Assert.assertTrue(elapsed >= 100);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    @ClientCompatibleTest
    public void testLatchDestroyed() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final Config config = new Config();
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
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
        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

        final ICountDownLatch latch1 = hz1.getCountDownLatch("test");
        latch1.trySetCount(10);
        Thread.sleep(100);

        final ICountDownLatch latch2 = hz2.getCountDownLatch("test");
        Assert.assertEquals(10, latch2.getCount());
        latch2.countDown();
        Assert.assertEquals(9, latch1.getCount());
        hz1.getLifecycleService().shutdown();
        Assert.assertEquals(9, latch2.getCount());

        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());
        final ICountDownLatch latch3 = hz3.getCountDownLatch("test");
        latch3.countDown();
        Assert.assertEquals(8, latch3.getCount());

        hz2.getLifecycleService().shutdown();
        latch3.countDown();
        Assert.assertEquals(7, latch3.getCount());

        HazelcastInstance hz4 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz5 = factory.newHazelcastInstance(new Config());
        Thread.sleep(250);

        hz3.getLifecycleService().shutdown();
        final ICountDownLatch latch4 = hz4.getCountDownLatch("test");
        Assert.assertEquals(7, latch4.getCount());

        final ICountDownLatch latch5 = hz5.getCountDownLatch("test");
        latch5.countDown();
        Assert.assertEquals(6, latch5.getCount());
        latch5.countDown();
        Assert.assertEquals(5, latch4.getCount());
        Assert.assertEquals(5, latch5.getCount());
    }
}
