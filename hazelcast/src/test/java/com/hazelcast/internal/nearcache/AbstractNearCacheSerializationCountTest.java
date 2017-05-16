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

package com.hazelcast.internal.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSizeEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.isCacheOnUpdate;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Contains the logic code for unified Near Cache serialization count tests.
 *
 * @param <NK> key type of the tested Near Cache
 * @param <NV> value type of the tested Near Cache
 */
public abstract class AbstractNearCacheSerializationCountTest<NK, NV> extends HazelcastTestSupport {

    /**
     * The default name used for the data structures which have a Near Cache.
     */
    protected static final String DEFAULT_NEAR_CACHE_NAME = "defaultNearCache";

    /**
     * Array filled with 1 ints.
     */
    protected static final int[] INT_ARRAY_1 = {1, 1, 1};

    /**
     * Array filled with 0 ints.
     */
    protected static final int[] INT_ARRAY_0 = {0, 0, 0};

    private static final AtomicInteger KEY_SERIALIZE_COUNT = new AtomicInteger();
    private static final AtomicInteger KEY_DESERIALIZE_COUNT = new AtomicInteger();
    private static final AtomicInteger VALUE_SERIALIZE_COUNT = new AtomicInteger();
    private static final AtomicInteger VALUE_DESERIALIZE_COUNT = new AtomicInteger();

    private static final AtomicReference<List<String>> KEY_SERIALIZE_STACKTRACE = new AtomicReference<List<String>>();
    private static final AtomicReference<List<String>> KEY_DESERIALIZE_STACKTRACE = new AtomicReference<List<String>>();
    private static final AtomicReference<List<String>> VALUE_SERIALIZE_STACKTRACE = new AtomicReference<List<String>>();
    private static final AtomicReference<List<String>> VALUE_DESERIALIZE_STACKTRACE = new AtomicReference<List<String>>();

    /**
     * An array with the expected number of key serializations for a {@link DataStructureAdapter#put}
     * and two {@link DataStructureAdapter#get(Object)} calls for the given {@link NearCacheConfig}.
     */
    protected int[] expectedKeySerializationCounts;

    /**
     * An array with the expected number of key deserializations for a {@link DataStructureAdapter#put}
     * and two {@link DataStructureAdapter#get(Object)} calls for the given {@link NearCacheConfig}.
     */
    protected int[] expectedKeyDeserializationCounts;

    /**
     * An array with the expected number of value serializations for a {@link DataStructureAdapter#put}
     * and two {@link DataStructureAdapter#get(Object)} calls for the given {@link NearCacheConfig}.
     */
    protected int[] expectedValueSerializationCounts;

    /**
     * An array with the expected number of value deserializations for a {@link DataStructureAdapter#put}
     * and two {@link DataStructureAdapter#get(Object)} calls for the given {@link NearCacheConfig}.
     */
    protected int[] expectedValueDeserializationCounts;

    /**
     * The {@link NearCacheConfig} used by the Near Cache tests.
     *
     * Needs to be set by the implementations of this class in their {@link org.junit.Before} methods.
     */
    protected NearCacheConfig nearCacheConfig;

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * @param <K> key type of the created {@link DataStructureAdapter}
     * @param <V> value type of the created {@link DataStructureAdapter}
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext();

    @Before
    public final void initStates() {
        KEY_SERIALIZE_COUNT.set(0);
        KEY_DESERIALIZE_COUNT.set(0);
        VALUE_SERIALIZE_COUNT.set(0);
        VALUE_DESERIALIZE_COUNT.set(0);

        KEY_SERIALIZE_STACKTRACE.set(new CopyOnWriteArrayList<String>());
        KEY_DESERIALIZE_STACKTRACE.set(new CopyOnWriteArrayList<String>());
        VALUE_SERIALIZE_STACKTRACE.set(new CopyOnWriteArrayList<String>());
        VALUE_DESERIALIZE_STACKTRACE.set(new CopyOnWriteArrayList<String>());
    }

    /**
     * Tests the serialization and deserialization counts on a {@link DataStructureAdapter} with a {@link NearCache}.
     */
    @Test
    public void testSerializationCounts() {
        NearCacheTestContext<KeySerializationCountingData, ValueSerializationCountingData, NK, NV> context = createContext();

        KeySerializationCountingData key = new KeySerializationCountingData();
        ValueSerializationCountingData value = new ValueSerializationCountingData();

        context.nearCacheAdapter.put(key, value);
        if (isCacheOnUpdate(nearCacheConfig)) {
            assertNearCacheSizeEventually(context, 1);
        }
        assertAndReset("put()", 0);

        context.nearCacheAdapter.get(key);
        assertAndReset("first get()", 1);

        context.nearCacheAdapter.get(key);
        assertAndReset("second get()", 2);
    }

    private void assertAndReset(String label, int index) {
        int expectedKeySerializeCount = expectedKeySerializationCounts[index];
        int expectedKeyDeserializeCount = expectedKeyDeserializationCounts[index];
        int expectedValueSerializeCount = expectedValueSerializationCounts[index];
        int expectedValueDeserializeCount = expectedValueDeserializationCounts[index];

        int actualKeySerializeCount = KEY_SERIALIZE_COUNT.getAndSet(0);
        int actualKeyDeserializeCount = KEY_DESERIALIZE_COUNT.getAndSet(0);
        int actualValueSerializeCount = VALUE_SERIALIZE_COUNT.getAndSet(0);
        int actualValueDeserializeCount = VALUE_DESERIALIZE_COUNT.getAndSet(0);

        List<String> keySerializeStackTrace = KEY_SERIALIZE_STACKTRACE.getAndSet(new CopyOnWriteArrayList<String>());
        List<String> keyDeserializeStackTrace = KEY_DESERIALIZE_STACKTRACE.getAndSet(new CopyOnWriteArrayList<String>());
        List<String> valueSerializeStackTrace = VALUE_SERIALIZE_STACKTRACE.getAndSet(new CopyOnWriteArrayList<String>());
        List<String> valueDeserializeStackTrace = VALUE_DESERIALIZE_STACKTRACE.getAndSet(new CopyOnWriteArrayList<String>());

        assertEquals(format("key serializeCount on %s: expected %d, but was %d%n%s",
                label, expectedKeySerializeCount, actualKeySerializeCount, keySerializeStackTrace),
                expectedKeySerializeCount, actualKeySerializeCount);
        assertEquals(format("key deserializeCount on %s: expected %d, but was %d%n%s",
                label, expectedKeyDeserializeCount, actualKeyDeserializeCount, keyDeserializeStackTrace),
                expectedKeyDeserializeCount, actualKeyDeserializeCount);

        assertEquals(format("value serializeCount on %s: expected %d, but was %d%n%s",
                label, expectedValueSerializeCount, actualValueSerializeCount, valueSerializeStackTrace),
                expectedValueSerializeCount, actualValueSerializeCount);
        assertEquals(format("value deserializeCount on %s: expected %d, but was %d%n%s",
                label, expectedValueDeserializeCount, actualValueDeserializeCount, valueDeserializeStackTrace),
                expectedValueDeserializeCount, actualValueDeserializeCount);
    }

    /**
     * Adds the serialization configuration for the used {@link Portable} domain object to the given {@link SerializationConfig}.
     *
     * @param serializationConfig the given {@link SerializationConfig} for the {@link DataStructureAdapter}
     */
    protected static void prepareSerializationConfig(SerializationConfig serializationConfig) {
        ClassDefinition keyClassDefinition = new ClassDefinitionBuilder(KeySerializationCountingData.FACTORY_ID,
                KeySerializationCountingData.CLASS_ID).build();
        serializationConfig.addClassDefinition(keyClassDefinition);

        ClassDefinition valueClassDefinition = new ClassDefinitionBuilder(ValueSerializationCountingData.FACTORY_ID,
                ValueSerializationCountingData.CLASS_ID).build();
        serializationConfig.addClassDefinition(valueClassDefinition);

        serializationConfig.addPortableFactory(KeySerializationCountingData.FACTORY_ID, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new KeySerializationCountingData();
            }
        });
        serializationConfig.addPortableFactory(ValueSerializationCountingData.FACTORY_ID, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new ValueSerializationCountingData();
            }
        });
    }

    private static String getStackTrace(String message) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        new HazelcastSerializationException(message).printStackTrace(pw);
        return sw.toString();
    }

    private static class KeySerializationCountingData implements Portable {

        private static int FACTORY_ID = 1;
        private static int CLASS_ID = 1;

        KeySerializationCountingData() {
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            KEY_SERIALIZE_COUNT.incrementAndGet();
            KEY_SERIALIZE_STACKTRACE.get().add(getStackTrace("invoked key serialization"));
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            KEY_DESERIALIZE_COUNT.incrementAndGet();
            KEY_DESERIALIZE_STACKTRACE.get().add(getStackTrace("invoked key deserialization"));
        }
    }

    private static class ValueSerializationCountingData implements Portable {

        private static int FACTORY_ID = 2;
        private static int CLASS_ID = 2;

        ValueSerializationCountingData() {
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            VALUE_SERIALIZE_COUNT.incrementAndGet();
            VALUE_SERIALIZE_STACKTRACE.get().add(getStackTrace("invoked value serialization"));
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            VALUE_DESERIALIZE_COUNT.incrementAndGet();
            VALUE_DESERIALIZE_STACKTRACE.get().add(getStackTrace("invoked value deserialization"));
        }
    }
}
