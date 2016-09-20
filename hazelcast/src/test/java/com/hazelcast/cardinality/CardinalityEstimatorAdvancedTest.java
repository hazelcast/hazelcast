package com.hazelcast.cardinality;

import com.hazelcast.core.HazelcastInstance;
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
public class CardinalityEstimatorAdvancedTest extends HazelcastTestSupport {

    @Test
    public void testCardinalityEstimatorFailure() {
        int k = 4;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(k + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        String name = "testFailure";
        CardinalityEstimator estimator = instance.getCardinalityEstimator(name);
        estimator.aggregate(1L);
        for (int i = 0; i < k; i++) {
            HazelcastInstance newInstance = nodeFactory.newHazelcastInstance();
            CardinalityEstimator newEstimator = newInstance.getCardinalityEstimator(name);
            assertEquals((long) 1 + i, newEstimator.estimate());
            newEstimator.aggregate(String.valueOf(i + 1));
            instance.shutdown();
            instance = newInstance;
        }
    }

    @Test
    public void testCardinalityEstimatorSpawnNodeInParallel() throws InterruptedException {
        int total = 6;
        int parallel = 2;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(total + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        final String name = "testSpawnNodeInParallel";
        CardinalityEstimator estimator = instance.getCardinalityEstimator(name);
        estimator.aggregate(1L);
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
                                        .aggregate(String.valueOf(counter.get()));
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
            }
        } finally {
            ex.shutdownNow();
        }
    }
}
