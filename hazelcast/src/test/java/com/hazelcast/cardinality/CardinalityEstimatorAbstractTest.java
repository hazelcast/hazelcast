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

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public abstract class CardinalityEstimatorAbstractTest extends HazelcastTestSupport {

    @Parameters(name = "config:{0}")
    public static Collection<Object[]> params() {
        final Config config = new Config();
        final SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setImplementation(new CustomObjectSerializer());
        serializerConfig.setTypeClass(CustomObject.class);
        config.getSerializationConfig().addSerializerConfig(serializerConfig);

        return asList(new Object[][]{
                {null},
                {config},
        });
    }

    protected HazelcastInstance[] instances;

    private CardinalityEstimator estimator;

    @Parameter(0)
    public Config config;

    @Before
    public void setup() {
        instances = newInstances(config);
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        estimator = local.getCardinalityEstimator(name);
    }

    protected abstract HazelcastInstance[] newInstances(Config config);

    @Test
    public void estimate() {
        assertEquals(0, estimator.estimate());
    }

    @Test
    public void estimateAsync() throws Exception {
        assertEquals(0, estimator.estimateAsync().toCompletableFuture().get().longValue());
    }

    @Test
    public void add() {
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

    @Test(expected = com.hazelcast.nio.serialization.HazelcastSerializationException.class)
    public void addCustomObject() {
        assumeTrue(config == null);

        estimator.add(new CustomObject(1, 2));
    }

    @Test()
    public void addCustomObjectRegisteredAsync() throws Exception {
        assumeTrue(config != null);

        assertEquals(0L, estimator.estimate());
        estimator.add(new CustomObject(1, 2));
        assertEquals(1L, estimator.estimate());
    }

    @Test()
    public void addCustomObjectRegistered() {
        assumeTrue(config != null);

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

    private static class CustomObjectSerializer implements StreamSerializer<CustomObject> {

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
