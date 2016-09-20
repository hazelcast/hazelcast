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

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
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

    private HazelcastInstance createSerializationConfiguredClient() {
        final SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setImplementation(new CustomObjectSerializer());
        serializerConfig.setTypeClass(CustomObject.class);

        ClientConfig config = new ClientConfig();
        config.getSerializationConfig().addSerializerConfig(serializerConfig);

        return hazelcastFactory.newHazelcastClient(config);
    }

    @Test
    public void test() throws Exception {
        assertEquals(0, estimator.estimate());
        assertEquals(true, estimator.aggregate(1L));
        assertEquals(true, estimator.aggregate(1L));
        assertEquals(1, estimator.estimate());

        for (long l : new long[] { 2L, 3L, 4L }) {
            assertEquals(true, estimator.aggregate(l));
        }

        assertEquals(4, estimator.estimate());
        assertEquals(true, estimator.aggregate("Test"));
        assertEquals(5, estimator.estimate());
    }

    @Test
    public void testAsync() throws Exception {
        ICompletableFuture<Long> f1 = estimator.estimateAsync();
        assertEquals(0L, f1.get().longValue());

        ICompletableFuture<Boolean> f2 = estimator.aggregateAsync(1L);
        assertEquals(true, f2.get());

        estimator.aggregateAsync(1L).get();
        f1 = estimator.estimateAsync();
        assertEquals(1L, f1.get().longValue());

        estimator.aggregateAsync(2L).get();
        estimator.aggregateAsync(3L).get();
        estimator.aggregateAsync(4L).get();

        f1 = estimator.estimateAsync();
        assertEquals(4, f1.get().longValue());

        f2 = estimator.aggregateAsync("Test");
        assertEquals(true, f2.get());

        estimator.aggregateAsync(1L).get();
        f1 = estimator.estimateAsync();
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
        assertEquals(1L, estimator.estimate());
    }

    @Test
    public void aggregateAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateAsync");
        assertEquals(true, estimator.aggregateAsync(10000L).get());
        assertEquals(1L, estimator.estimateAsync().get().longValue());
    }

    @Test
    public void aggregateString() {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateString");
        assertEquals(true, estimator.aggregate("String1"));
        assertEquals(1L, estimator.estimate());
    }

    @Test
    public void aggregateStringAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateStringAsync");
        assertEquals(true, estimator.aggregateAsync("String1").get());
        assertEquals(1L, estimator.estimateAsync().get().longValue());
    }

    @Test(expected = com.hazelcast.nio.serialization.HazelcastSerializationException.class)
    public void aggregateCustomObject() {
        CardinalityEstimator estimator = client.getCardinalityEstimator("aggregateCustomObject");
        assertEquals(true, estimator.aggregate(new CustomObject(1, 2)));
    }

    @Test()
    public void aggregateCustomObjectAsync()
            throws ExecutionException, InterruptedException {
        CardinalityEstimator estimator = createSerializationConfiguredClient().getCardinalityEstimator("aggregateCustomObjectAsync");
        assertEquals(0L, estimator.estimate());
        assertEquals(true, estimator.aggregate(new CustomObject(1, 2)));
        assertEquals(1L, estimator.estimate());
    }

    @Test()
    public void aggregateCustomObjectRegistered() {
        CardinalityEstimator estimator = createSerializationConfiguredClient().getCardinalityEstimator("aggregateCustomObject");
        assertEquals(0L, estimator.estimate());
        assertEquals(true, estimator.aggregate(new CustomObject(1, 2)));
        assertEquals(1L, estimator.estimate());
    }

    private class CustomObject {
        private final int x;
        private final int y;

        private CustomObject(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    private class CustomObjectSerializer implements StreamSerializer<CustomObject> {

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void destroy() {
        }

        @Override
        public void write(ObjectDataOutput out, CustomObject object)
                throws IOException {
            out.writeLong((object.x << Bits.INT_SIZE_IN_BYTES) | object.y);
        }

        @Override
        public CustomObject read(ObjectDataInput in)
                throws IOException {
            // Not needed
            throw new UnsupportedOperationException();
        }
    }

}
