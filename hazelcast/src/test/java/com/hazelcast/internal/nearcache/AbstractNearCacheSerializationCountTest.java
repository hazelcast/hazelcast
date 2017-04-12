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
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.warmupPartitionsAndWaitForAllSafeState;
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

    private static final AtomicInteger SERIALIZE_COUNT = new AtomicInteger();
    private static final AtomicInteger DESERIALIZE_COUNT = new AtomicInteger();

    private static final AtomicReference<List<String>> SERIALIZE_STACKTRACE = new AtomicReference<List<String>>();
    private static final AtomicReference<List<String>> DESERIALIZE_STACKTRACE = new AtomicReference<List<String>>();

    /**
     * The {@link NearCacheConfig} used by the Near Cache tests.
     *
     * Needs to be set by the implementations of this class in their {@link org.junit.Before} methods.
     */
    protected NearCacheConfig nearCacheConfig;

    /**
     * An array with the expected number of serializations for a {@link DataStructureAdapter#put}
     * and two {@link DataStructureAdapter#get(Object)} calls for the given {@link NearCacheConfig}.
     *
     * @return an array with expected serialization counts
     */
    protected abstract int[] getExpectedSerializationCounts();

    /**
     * An array with the expected number of deserializations for a {@link DataStructureAdapter#put}
     * and two {@link DataStructureAdapter#get(Object)} calls for the given {@link NearCacheConfig}.
     *
     * @return an array with expected deserialization counts
     */
    protected abstract int[] getExpectedDeserializationCounts();

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
        DESERIALIZE_COUNT.set(0);
        SERIALIZE_COUNT.set(0);
        SERIALIZE_STACKTRACE.set(new CopyOnWriteArrayList<String>());
        DESERIALIZE_STACKTRACE.set(new CopyOnWriteArrayList<String>());
    }

    /**
     * Tests the serialization and deserialization counts on a {@link DataStructureAdapter} with a {@link NearCache}.
     */
    @Test
    public void testSerializationCounts() {
        NearCacheTestContext<String, SerializationCountingData, NK, NV> context = createContext();
        warmupPartitionsAndWaitForAllSafeState(context);

        String key = randomString();
        SerializationCountingData value = new SerializationCountingData();

        context.nearCacheAdapter.put(key, value);
        if (isCacheOnUpdate(context)) {
            assertNearCacheSizeEventually(context, 1);
        }
        assertAndReset("put()", getExpectedSerializationCounts()[0], getExpectedDeserializationCounts()[0]);

        context.nearCacheAdapter.get(key);
        assertAndReset("first get()", getExpectedSerializationCounts()[1], getExpectedDeserializationCounts()[1]);

        context.nearCacheAdapter.get(key);
        assertAndReset("second get()", getExpectedSerializationCounts()[2], getExpectedDeserializationCounts()[2]);
    }

    /**
     * Adds the serialization configuration for the used {@link Portable} domain object to the given {@link SerializationConfig}.
     *
     * @param serializationConfig the given {@link SerializationConfig} for the {@link DataStructureAdapter}
     */
    protected static void prepareSerializationConfig(SerializationConfig serializationConfig) {
        ClassDefinition classDefinition = new ClassDefinitionBuilder(SerializationCountingData.FACTORY_ID,
                SerializationCountingData.CLASS_ID).build();
        serializationConfig.addClassDefinition(classDefinition);

        serializationConfig.addPortableFactory(SerializationCountingData.FACTORY_ID, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new SerializationCountingData();
            }
        });
    }

    private static void assertAndReset(String label, int serializeCount, int deserializeCount) {
        int actualSerializeCount = SERIALIZE_COUNT.getAndSet(0);
        int actualDeserializeCount = DESERIALIZE_COUNT.getAndSet(0);
        List<String> serializeStackTrace = SERIALIZE_STACKTRACE.getAndSet(new CopyOnWriteArrayList<String>());
        List<String> deserializeStackTrace = DESERIALIZE_STACKTRACE.getAndSet(new CopyOnWriteArrayList<String>());
        assertEquals(format("serializeCount on %s: expected %d, but was %d%n%s",
                label, serializeCount, actualSerializeCount, serializeStackTrace),
                serializeCount, actualSerializeCount);
        assertEquals(format("deserializeCount on %s: expected %d, but was %d%n%s",
                label, deserializeCount, actualDeserializeCount, deserializeStackTrace),
                deserializeCount, actualDeserializeCount);
    }

    private static class SerializationCountingData implements Portable {

        private static int FACTORY_ID = 1;
        private static int CLASS_ID = 1;

        SerializationCountingData() {
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
            SERIALIZE_COUNT.incrementAndGet();
            SERIALIZE_STACKTRACE.get().add(getStackTrace("invoked serialization"));
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            DESERIALIZE_COUNT.incrementAndGet();
            DESERIALIZE_STACKTRACE.get().add(getStackTrace("invoked deserialization"));
        }

        private static String getStackTrace(String message) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            new HazelcastSerializationException(message).printStackTrace(pw);
            return sw.toString();
        }
    }
}
