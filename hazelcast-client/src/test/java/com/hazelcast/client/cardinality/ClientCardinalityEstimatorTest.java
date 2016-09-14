/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cardinality;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.CardinalityEstimator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCardinalityEstimatorTest
        extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private CardinalityEstimator estimator;

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
        estimator = client.getCardinalityEstimator(randomString());
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test() throws Exception {
        assertEquals(0, estimator.estimate());
        assertEquals(1, estimator.aggregateAndEstimate(1L));
        assertEquals(1, estimator.aggregateAndEstimate(1L));
        assertEquals(true, estimator.aggregateAll(new long[] { 2L, 3L }));
        assertEquals(4, estimator.aggregateAllAndEstimate(new long[] { 4L }));
        assertEquals(4, estimator.estimate());
        assertEquals(true, estimator.aggregateString("Test"));
        assertEquals(5, estimator.estimate());
    }

    @Test
    public void testAsync() throws Exception {
        ICompletableFuture<Long> f1 = estimator.estimateAsync();
        assertEquals(0L, f1.get().longValue());

        f1 = estimator.aggregateAndEstimateAsync(1L);
        assertEquals(1L, f1.get().longValue());

        f1 = estimator.aggregateAndEstimateAsync(1L);
        assertEquals(1L, f1.get().longValue());

        ICompletableFuture<Boolean> f2 = estimator.aggregateAllAsync(new long[] { 2L, 3L });
        assertEquals(true, f2.get());

        f1 = estimator.aggregateAllAndEstimateAsync(new long[] { 4L });
        assertEquals(4, f1.get().longValue());

        f1 = estimator.estimateAsync();
        assertEquals(4, f1.get().longValue());

        f2 = estimator.aggregateStringAsync("Test");
        assertEquals(true, f2.get());

        f1 = estimator.aggregateAndEstimateAsync(1L);
        assertEquals(5L, f1.get().longValue());
    }

    @Test
    public void estimate() {
        CardinalityEstimator estimator = client.getCardinalityEstimator("estimate");
        assertEquals(0, estimator.estimate());
    }

    @Test
    public void estimateAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("estimateAsync");
        assertEquals(0, estimator.estimateAsync().get().longValue());
    }

    @Test
    public void aggregate() {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregate");
        assertEquals(true, estimator.aggregate(1L));
    }

    @Test
    public void aggregateAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateAsync");
        assertEquals(true, estimator.aggregateAsync(10000L).get().booleanValue());
    }

    @Test
    public void aggregateAll() {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateAll");
        assertEquals(true, estimator.aggregateAll(new long[] { 1L, 2L, 3L }));
    }

    @Test
    public void aggregateAllAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateAllAsync");
        assertEquals(true, estimator.aggregateAllAsync(new long[] { 1L, 2L, 3L }).get().booleanValue());
    }

    @Test
    public void aggregateAndEstimateAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateAndEstimateAsync");
        assertEquals(1L, estimator.aggregateAndEstimateAsync(1000L).get().longValue());
    }

    @Test
    public void aggregateAllAndEstimateAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateAllAndEstimateAsync");
        assertEquals(3L, estimator.aggregateAllAndEstimateAsync(new long[] { 1L, 2L, 3L }).get().longValue());
    }

    @Test
    public void aggregateString() {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateString");
        assertEquals(true, estimator.aggregateString("String1"));
    }

    @Test
    public void aggregateStringAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateStringAsync");
        assertEquals(true, estimator.aggregateStringAsync("String1").get().booleanValue());
    }

    @Test
    public void aggregateAllStrings() {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateAllStrings");
        assertEquals(true, estimator.aggregateAllStrings(new String[] { "String1", "String2", "String3" }));
    }

    @Test
    public void aggregateAllStringsAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateAllStringsAsync");
        assertEquals(true, estimator.aggregateAllStringsAsync(new String[] { "String1", "String2", "String3" }).get()
                                    .booleanValue());
    }

}
