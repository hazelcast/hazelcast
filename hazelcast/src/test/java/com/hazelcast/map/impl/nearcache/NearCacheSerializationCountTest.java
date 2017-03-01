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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NearCacheSerializationCountTest extends HazelcastTestSupport {

    protected static final String MAP_NAME = randomString();
    protected static final AtomicInteger SERIALIZE_COUNT = new AtomicInteger();
    protected static final AtomicInteger DESERIALIZE_COUNT = new AtomicInteger();

    protected TestHazelcastInstanceFactory hazelcastFactory = new TestHazelcastInstanceFactory();
    protected IMap<String, SerializationCountingData> map;

    @After
    public void tearDown() {
        DESERIALIZE_COUNT.set(0);
        SERIALIZE_COUNT.set(0);
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testDeserializationCountWith_ObjectNearCache_invalidateLocalUpdatePolicy() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(OBJECT, INVALIDATE);
        prepareCache(nearCacheConfig);

        String key = randomString();
        SerializationCountingData value = new SerializationCountingData();
        map.put(key, value);
        assertAndReset(1, 0);

        map.get(key);
        assertAndReset(0, 1);

        map.get(key);
        assertAndReset(0, 0);
    }

    @Test
    public void testDeserializationCountWith_BinaryNearCache_invalidateLocalUpdatePolicy() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(BINARY, INVALIDATE);
        prepareCache(nearCacheConfig);

        String key = randomString();
        SerializationCountingData value = new SerializationCountingData();
        map.put(key, value);
        assertAndReset(1, 0);

        map.get(key);
        assertAndReset(0, 1);

        map.get(key);
        assertAndReset(0, 1);
    }

    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat, LocalUpdatePolicy localUpdatePolicy) {
        return new NearCacheConfig()
                .setName(MAP_NAME)
                .setLocalUpdatePolicy(localUpdatePolicy)
                .setInMemoryFormat(inMemoryFormat).setCacheLocalEntries(true);
    }

    protected Config createConfig() {
        Config config = new Config();
        SerializationConfig serializationConfig = config.getSerializationConfig();
        prepareSerializationConfig(serializationConfig);
        return config;
    }

    protected void prepareSerializationConfig(SerializationConfig serializationConfig) {
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

    protected void prepareCache(NearCacheConfig nearCacheConfig) {
        hazelcastFactory.newHazelcastInstance(createConfig());

        Config config = createConfig();
        config.getMapConfig(MAP_NAME).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        map = member.getMap(MAP_NAME);
    }

    protected void assertAndReset(int serializeCount, int deserializeCount) {
        assertEquals(serializeCount, SERIALIZE_COUNT.getAndSet(0));
        assertEquals(deserializeCount, DESERIALIZE_COUNT.getAndSet(0));
    }

    protected static class SerializationCountingData implements Portable {

        static int FACTORY_ID = 1;
        static int CLASS_ID = 1;

        public SerializationCountingData() {
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
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            DESERIALIZE_COUNT.incrementAndGet();
        }
    }
}
