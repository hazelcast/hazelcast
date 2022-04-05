/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class CardinalityEstimatorAdvancedTest extends HazelcastTestSupport {

    @Test
    public void testCardinalityEstimatorFailure() {
        int k = 4;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(k + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        String name = "testFailure";
        CardinalityEstimator estimator = instance.getCardinalityEstimator(name);
        estimator.add(1L);
        for (int i = 0; i < k; i++) {
            HazelcastInstance newInstance = nodeFactory.newHazelcastInstance();
            waitAllForSafeState(instance, newInstance);
            CardinalityEstimator newEstimator = newInstance.getCardinalityEstimator(name);
            assertEquals((long) 1 + i, newEstimator.estimate());
            newEstimator.add(String.valueOf(i + 1));
            instance.shutdown();
            instance = newInstance;
        }
    }

    @Test
    public void testCardinalityEstimatorFailure_whenSwitchedToDense() {
        int k = 4;
        int sparseLimit = 10000;

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(k + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        String name = "testFailure_whenSwitchedToDense";
        CardinalityEstimator estimator = instance.getCardinalityEstimator(name);
        for (int i = 0; i < sparseLimit; i++) {
            estimator.add(String.valueOf(i + 1));
        }

        long estimateSoFar = estimator.estimate();

        for (int i = 0; i < k; i++) {
            HazelcastInstance newInstance = nodeFactory.newHazelcastInstance();
            waitAllForSafeState(instance, newInstance);
            CardinalityEstimator newEstimator = newInstance.getCardinalityEstimator(name);
            assertEquals(estimateSoFar, newEstimator.estimate());

            // Add numbers with huge gaps in between to guarantee change of est.
            newEstimator.add(String.valueOf(1 << (14 + i)));
            estimateSoFar = newEstimator.estimate();
            instance.shutdown();
            instance = newInstance;
        }
    }

    @Test
    public void testCardinalityEstimatorSpawnNodeInParallel() {
        int total = 6;
        int parallel = 2;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(total + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        final String name = "testSpawnNodeInParallel";
        CardinalityEstimator estimator = instance.getCardinalityEstimator(name);
        estimator.add(1L);
        final ExecutorService ex = Executors.newFixedThreadPool(parallel);
        try {
            for (int i = 0; i < total / parallel; i++) {
                final HazelcastInstance[] instances = new HazelcastInstance[parallel];
                final CountDownLatch countDownLatch = new CountDownLatch(parallel);
                final AtomicInteger counter = new AtomicInteger(0);
                final AtomicInteger exceptionCount = new AtomicInteger(0);
                for (int j = 0; j < parallel; j++) {
                    final int id = j;
                    ex.execute(new Runnable() {
                        public void run() {
                            try {
                                counter.incrementAndGet();
                                instances[id] = nodeFactory.newHazelcastInstance();
                                instances[id].getCardinalityEstimator(name)
                                        .add(String.valueOf(counter.get()));
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

                // if there is an exception while incrementing in parallel threads, find number of exceptions
                // and subtract the number from expectedValue.
                final int thrownExceptionCount = exceptionCount.get();
                final long expectedValue = (long) counter.get() - thrownExceptionCount;
                CardinalityEstimator newEstimator = instance.getCardinalityEstimator(name);
                assertEquals(expectedValue, newEstimator.estimate());
                instance.shutdown();
                instance = instances[0];
                waitAllForSafeState(instances);
            }
        } finally {
            ex.shutdownNow();
        }
    }
}
