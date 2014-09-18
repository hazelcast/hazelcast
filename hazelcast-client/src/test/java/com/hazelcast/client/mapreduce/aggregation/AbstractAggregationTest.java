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

package com.hazelcast.client.mapreduce.aggregation;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.aggregation.PropertyExtractor;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Random;

public class AbstractAggregationTest
        extends HazelcastTestSupport {

    private static final int VALUES_COUNT = 10000;
    private static final Random RANDOM = new Random();

    protected static HazelcastInstance HAZELCAST_INSTANCE;

    @BeforeClass
    public static void startup() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();

        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        HAZELCAST_INSTANCE = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() {
        for (DistributedObject object : HAZELCAST_INSTANCE.getDistributedObjects()) {
            if (object instanceof IMap) {
                ((IMap) object).destroy();
            }
        }
    }

    protected static int random(int min, int max) {
        int delta = max - min;
        return min + RANDOM.nextInt(delta);
    }

    protected static <T> T[] buildPlainValues(ValueProvider<T> valueProvider, Class<T> type) {
        T[] values = (T[]) Array.newInstance(type, VALUES_COUNT);
        for (int i = 0; i < VALUES_COUNT; i++) {
            values[i] = valueProvider.provideRandom(RANDOM);
        }
        return values;
    }

    protected static <T> Value<T>[] buildValues(ValueProvider<T> valueProvider) {
        Value<T>[] values = new Value[VALUES_COUNT];
        for (int i = 0; i < VALUES_COUNT; i++) {
            T value = valueProvider.provideRandom(RANDOM);
            values[i] = value(value);
        }
        return values;
    }

    private static <T> Value<T> value(T value) {
        return new Value<T>(value);
    }

    protected static interface ValueProvider<T> {
        T provideRandom(Random random);
    }

    public static class ValuePropertyExtractor<T>
            implements PropertyExtractor<Value<T>, T>, Serializable {

        @Override
        public T extract(Value<T> value) {
            return value.value;
        }
    }

    public static class Value<T>
            implements Serializable {

        public T value;

        public Value() {
        }

        public Value(T value) {
            this.value = value;
        }
    }
}
