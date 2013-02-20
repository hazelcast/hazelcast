/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.atomicnumber.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * User: sancar
 * Date: 12/31/12
 * Time: 10:26 AM
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class AtomicNumberTest {

    @Before
    @After
    public void shutdown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSimpleAtomicLong() {
        HazelcastInstance hazelcastInstance = new StaticNodeFactory(1).newInstance(new Config());
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
    public void testMultipleThreadAtomicLong() {

        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(new Config(), 1);
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
            countDownLatch.await(5, TimeUnit.SECONDS);
            Assert.assertEquals(0, atomicLong.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}