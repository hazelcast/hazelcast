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

package com.hazelcast.cardinality;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICardinalityEstimator;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public abstract class CardinalityEstimatorAbstractTest
        extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;

    private ICardinalityEstimator estimator;

    @Before
    public void setup() {
        instances = newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        estimator = local.getCardinalityEstimator(name);
    }

    protected abstract HazelcastInstance[] newInstances();

    @Test
    public void test() {
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
        assertEquals(0, estimator.estimate());
    }

    @Test
    public void estimateAsync()
            throws ExecutionException, InterruptedException {
        assertEquals(0, estimator.estimateAsync().get().longValue());
    }

    @Test
    public void aggregate() {
        assertEquals(true, estimator.aggregate(1L));
    }

    @Test
    public void aggregateAsync()
            throws ExecutionException, InterruptedException {
        assertEquals(true, estimator.aggregateAsync(10000L).get().booleanValue());
    }

    @Test
    public void aggregateAll() {
        assertEquals(true, estimator.aggregateAll(new long[] { 1L, 2L, 3L }));
    }

    @Test
    public void aggregateAllAsync()
            throws ExecutionException, InterruptedException {
        assertEquals(true, estimator.aggregateAllAsync(new long[] { 1L, 2L, 3L }).get().booleanValue());
    }

    @Test
    public void aggregateAndEstimateAsync()
            throws ExecutionException, InterruptedException {
        assertEquals(1L, estimator.aggregateAndEstimateAsync(1000L).get().longValue());
    }

    @Test
    public void aggregateAllAndEstimateAsync()
            throws ExecutionException, InterruptedException {
        assertEquals(3L, estimator.aggregateAllAndEstimateAsync(new long[] { 1L, 2L, 3L }).get().longValue());
    }

    @Test
    public void aggregateString() {
        assertEquals(true, estimator.aggregateString("String1"));
    }

    @Test
    public void aggregateStringAsync()
            throws ExecutionException, InterruptedException {
        assertEquals(true, estimator.aggregateStringAsync("String1").get().booleanValue());
    }

    @Test
    public void aggregateAllStrings() {
        assertEquals(true, estimator.aggregateAllStrings(new String[] { "String1", "String2", "String3" }));
    }

    @Test
    public void aggregateAllStringsAsync()
            throws ExecutionException, InterruptedException {
        assertEquals(true, estimator.aggregateAllStringsAsync(new String[] { "String1", "String2", "String3" }).get()
                                    .booleanValue());
    }
}
