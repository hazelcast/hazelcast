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

package com.hazelcast.client.cardinality;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCardinalityEstimatorTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private CardinalityEstimator estimator;

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    private HazelcastInstance createSerializationConfiguredClient() {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setImplementation(new CustomObjectSerializer());
        serializerConfig.setTypeClass(CustomObject.class);

        ClientConfig config = new ClientConfig();
        config.getSerializationConfig().addSerializerConfig(serializerConfig);

        return hazelcastFactory.newHazelcastClient(config);
    }

    @Test
    public void estimate() {
        estimator = client.getCardinalityEstimator("estimate");
        assertEquals(0, estimator.estimate());
    }

    @Test
    public void estimateAsync() throws Exception {
        estimator = client.getCardinalityEstimator("estimateAsync");
        assertEquals(0, estimator.estimateAsync().toCompletableFuture().get().longValue());
    }

    @Test
    public void add() {
        estimator = client.getCardinalityEstimator("aggregate");
        estimator.add(1L);
        assertEquals(1L, estimator.estimate());
        estimator.add(1L);
        estimator.add(1L);
        assertEquals(1L, estimator.estimate());
        estimator.add(2L);
        estimator.add(3L);
        assertEquals(3L, estimator.estimate());
        estimator.add("Test");
        assertEquals(4L, estimator.estimate());
    }

    @Test
    public void addAsync() throws Exception {
        estimator = client.getCardinalityEstimator("aggregateAsync");
        estimator.addAsync(1L).toCompletableFuture().get();
        assertEquals(1L, estimator.estimateAsync().toCompletableFuture().get().longValue());
        estimator.addAsync(1L).toCompletableFuture().get();
        estimator.addAsync(1L).toCompletableFuture().get();
        assertEquals(1L, estimator.estimateAsync().toCompletableFuture().get().longValue());
        estimator.addAsync(2L).toCompletableFuture().get();
        estimator.addAsync(3L);
        assertEquals(3L, estimator.estimateAsync().toCompletableFuture().get().longValue());
        estimator.addAsync("Test").toCompletableFuture().get();
        assertEquals(4L, estimator.estimateAsync().toCompletableFuture().get().longValue());
    }

    @Test
    public void addString() {
        estimator = client.getCardinalityEstimator("aggregateString");
        estimator.add("String1");
        assertEquals(1L, estimator.estimate());
    }

    @Test
    public void addStringAsync() throws Exception {
        estimator = client.getCardinalityEstimator("aggregateStringAsync");
        estimator.addAsync("String1").toCompletableFuture().get();
        assertEquals(1L, estimator.estimateAsync().toCompletableFuture().get().longValue());
    }

    @Test(expected = com.hazelcast.nio.serialization.HazelcastSerializationException.class)
    public void addCustomObject() {
        estimator = client.getCardinalityEstimator("aggregateCustomObject");
        estimator.add(new CustomObject(1, 2));
    }

    @Test
    public void addCustomObjectRegisteredAsync() {
        estimator = createSerializationConfiguredClient().getCardinalityEstimator("aggregateCustomObjectAsync");
        assertEquals(0L, estimator.estimate());
        estimator.add(new CustomObject(1, 2));
        assertEquals(1L, estimator.estimate());
    }

    @Test
    public void addCustomObjectRegistered() {
        estimator = createSerializationConfiguredClient().getCardinalityEstimator("aggregateCustomObject");
        assertEquals(0L, estimator.estimate());
        estimator.add(new CustomObject(1, 2));
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
        public void write(ObjectDataOutput out, CustomObject object) throws IOException {
            out.writeLong((object.x << Bits.INT_SIZE_IN_BYTES) | object.y);
        }

        @Override
        public CustomObject read(ObjectDataInput in) throws IOException {
            // not needed
            throw new UnsupportedOperationException();
        }
    }
}
