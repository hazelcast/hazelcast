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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class AtomicLongTest extends HazelcastTestSupport {

    @Test
    @ClientCompatibleTest
    public void testSimpleAtomicLong() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicLong an = hazelcastInstance.getAtomicLong("testAtomicLong");
        assertEquals(0, an.get());
        assertEquals(-1, an.decrementAndGet());
        assertEquals(0, an.incrementAndGet());
        assertEquals(1, an.incrementAndGet());
        assertEquals(2, an.incrementAndGet());
        assertEquals(1, an.decrementAndGet());
        assertEquals(1, an.getAndSet(23));
        assertEquals(28, an.addAndGet(5));
        assertEquals(28, an.get());
        assertEquals(28, an.getAndAdd(-3));
        assertEquals(24, an.decrementAndGet());
        Assert.assertFalse(an.compareAndSet(23, 50));
        assertTrue(an.compareAndSet(24, 50));
        assertTrue(an.compareAndSet(50, 0));
    }

    @Test
    @ClientCompatibleTest
    public void testMultipleThreadAtomicLong() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final HazelcastInstance instance = instances[0];
        final int k = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(k);
        final IAtomicLong atomicLong = instance.getAtomicLong("testMultipleThreadAtomicLong");
        for (int i = 0; i < k; i++) {
            new Thread() {
                public void run() {
                    long delta = (long) (Math.random() * 1000);
                    for (int j = 0; j < 10000; j++) {
                        atomicLong.addAndGet(delta);
                    }
                    for (int j = 0; j < 10000; j++) {
                        atomicLong.addAndGet(-1 * delta);
                    }
                    countDownLatch.countDown();
                }
            }.start();
        }
        try {
            countDownLatch.await(50, TimeUnit.SECONDS);
            Assert.assertEquals(0, atomicLong.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    @ClientCompatibleTest
    public void testAtomicLongFailure() {
        int k = 4;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(k + 1);
        Config config = new Config();
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        String name = "testAtomicLongFailure";
        IAtomicLong atomicLong = instance.getAtomicLong(name);
        atomicLong.set(100);
        for (int i = 0; i < k; i++) {
            HazelcastInstance newInstance = nodeFactory.newHazelcastInstance(config);
            IAtomicLong newAtomicLong = newInstance.getAtomicLong(name);
            Assert.assertEquals((long) 100 + i, newAtomicLong.get());
            newAtomicLong.incrementAndGet();
            instance.getLifecycleService().shutdown();
            instance = newInstance;
        }
    }

    @Test
    @ClientCompatibleTest
    public void testAtomicLongSpawnNodeInParallel() {
        int total = 6;
        int parallel = 2;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(total + 1);
        final Config config = new Config();
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        final String name = "testAtomicLongSpawnNodeInParallel";
        IAtomicLong atomicLong = instance.getAtomicLong(name);
        atomicLong.set(100);
        for (int i = 0; i < total / parallel; i++) {
            final HazelcastInstance[] instances = new HazelcastInstance[parallel];
            final CountDownLatch countDownLatch = new CountDownLatch(parallel);
            for (int j = 0; j < parallel; j++) {
                final int id = j;
                new Thread() {
                    public void run() {
                        instances[id] = nodeFactory.newHazelcastInstance(config);
                        instances[id].getAtomicLong(name).incrementAndGet();
                        countDownLatch.countDown();
                    }
                }.start();
            }
            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            IAtomicLong newAtomicLong = instance.getAtomicLong(name);
            Assert.assertEquals((long) 100 + (i + 1) * parallel, newAtomicLong.get());
            instance.getLifecycleService().shutdown();
            instance = instances[0];
            for (int j = 1; j < parallel; j++) {
                instances[j].getLifecycleService().shutdown();
            }
        }
    }
}
