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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicLongAdvancedTest extends HazelcastTestSupport {

    @Test
    public void testMultipleThreadAtomicLong() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance();
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
        assertOpenEventually(countDownLatch, 300);
        assertEquals(0, atomicLong.get());
    }

    @Test
    public void testAtomicLongFailure() {
        int k = 4;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(k + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        String name = "testAtomicLongFailure";
        IAtomicLong atomicLong = instance.getAtomicLong(name);
        atomicLong.set(100);
        for (int i = 0; i < k; i++) {
            HazelcastInstance newInstance = nodeFactory.newHazelcastInstance();
            waitAllForSafeState(nodeFactory.getAllHazelcastInstances());

            IAtomicLong newAtomicLong = newInstance.getAtomicLong(name);
            assertEquals((long) 100 + i, newAtomicLong.get());
            newAtomicLong.incrementAndGet();
            instance.shutdown();
            instance = newInstance;
        }
    }

    @Test
    public void testAtomicLongSpawnNodeInParallel() throws InterruptedException {
        int total = 6;
        int parallel = 2;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(total + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        final String name = "testAtomicLongSpawnNodeInParallel";
        IAtomicLong atomicLong = instance.getAtomicLong(name);
        atomicLong.set(100);
        final ExecutorService ex = Executors.newFixedThreadPool(parallel);
        try {
            for (int i = 0; i < total / parallel; i++) {
                final HazelcastInstance[] instances = new HazelcastInstance[parallel];
                final CountDownLatch countDownLatch = new CountDownLatch(parallel);
                final AtomicInteger exceptionCount = new AtomicInteger(0);
                for (int j = 0; j < parallel; j++) {
                    final int id = j;
                    ex.execute(new Runnable() {
                        public void run() {
                            try {
                                instances[id] = nodeFactory.newHazelcastInstance();
                                instances[id].getAtomicLong(name).incrementAndGet();
                            } catch (Exception e) {
                                exceptionCount.incrementAndGet();
                                e.printStackTrace();
                            } finally {
                                countDownLatch.countDown();
                            }
                        }
                    });
                }
                assertOpenEventually(countDownLatch);
                waitAllForSafeState(nodeFactory.getAllHazelcastInstances());

                // if there is an exception while incrementing in parallel threads, find number of exceptions
                // and subtract the number from expectedValue.
                final int thrownExceptionCount = exceptionCount.get();
                final long expectedValue = (long) 100 + (i + 1) * parallel - thrownExceptionCount;
                IAtomicLong newAtomicLong = instance.getAtomicLong(name);
                assertEquals(expectedValue, newAtomicLong.get());
                instance.shutdown();
                instance = instances[0];
            }
        } finally {
            ex.shutdownNow();
        }
    }
}
