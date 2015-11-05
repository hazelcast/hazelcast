/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.nearcache;


import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientCacheSerializationCountTest extends HazelcastTestSupport {

    static final String CACHE_NAME = randomString();
    TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    NearCache nearCache;
    ICache<String, SerializationCountingData> cache;

    private static final AtomicInteger serializeCount = new AtomicInteger();
    private static final AtomicInteger deserializeCount = new AtomicInteger();

    static class SerializationCountingData implements Portable {
        public static int FACTORY_ID = 1;
        public static int CLASS_ID = 1;

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
            serializeCount.incrementAndGet();
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            deserializeCount.incrementAndGet();
        }

    }

    @After
    public void tearDown() {
        deserializeCount.set(0);
        serializeCount.set(0);
        hazelcastFactory.terminateAll();
    }

    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        return new CacheConfig()
                .setName(CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat
            , NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        return new NearCacheConfig()
                .setName(CACHE_NAME)
                .setLocalUpdatePolicy(localUpdatePolicy)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected Config createConfig() {
        Config config = new Config();
        SerializationConfig serializationConfig = config.getSerializationConfig();
        prepareSerializationConfig(serializationConfig);
        return config;
    }

    protected ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();
        SerializationConfig serializationConfig = config.getSerializationConfig();
        prepareSerializationConfig(serializationConfig);
        return config;
    }

    private void prepareSerializationConfig(SerializationConfig serializationConfig) {
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

    @Test
    public void testDeserializationCountWith_ObjectNearCache_cacheLocalUpdatePolicy() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(InMemoryFormat.OBJECT
                , NearCacheConfig.LocalUpdatePolicy.CACHE);
        prepareCache(nearCacheConfig);

        final String key = randomString();
        SerializationCountingData value = new SerializationCountingData();
        cache.put(key, value);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, nearCache.size());
            }
        });
        assertAndReset(1, 0);

        cache.get(key);
        assertAndReset(0, 0);

        cache.get(key);
        assertAndReset(0, 0);
    }

    @Test
    public void testDeserializationCountWith_BinaryNearCache_cacheLocalUpdatePolicy() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(InMemoryFormat.BINARY
                , NearCacheConfig.LocalUpdatePolicy.CACHE);
        prepareCache(nearCacheConfig);

        String key = randomString();
        SerializationCountingData value = new SerializationCountingData();
        cache.put(key, value);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, nearCache.size());
            }
        });
        assertAndReset(1, 0);

        cache.get(key);
        assertAndReset(0, 1);

        cache.get(key);
        assertAndReset(0, 1);
    }

    @Test
    public void testDeserializationCountWith_ObjectNearCache_invalidateLocalUpdatePolicy() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(InMemoryFormat.OBJECT
                , NearCacheConfig.LocalUpdatePolicy.INVALIDATE);
        prepareCache(nearCacheConfig);

        String key = randomString();
        SerializationCountingData value = new SerializationCountingData();
        cache.put(key, value);
        assertAndReset(1, 0);

        cache.get(key);
        assertAndReset(0, 1);

        cache.get(key);
        assertAndReset(0, 0);
    }

    @Test
    public void testDeserializationCountWith_BinaryNearCache_invalidateLocalUpdatePolicy() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(InMemoryFormat.BINARY
                , NearCacheConfig.LocalUpdatePolicy.INVALIDATE);
        prepareCache(nearCacheConfig);

        String key = randomString();
        SerializationCountingData value = new SerializationCountingData();
        cache.put(key, value);
        assertAndReset(1, 0);

        cache.get(key);
        assertAndReset(0, 1);

        cache.get(key);
        assertAndReset(0, 1);
    }

    @Test
    public void testDeserializationCountWithoutNearCache() {
        prepareCache(null);

        SerializationCountingData value = new SerializationCountingData();

        String key = randomString();
        cache.put(key, value);
        assertAndReset(1, 0);

        cache.get(key);
        assertAndReset(0, 1);

        cache.get(key);
        assertAndReset(0, 1);
    }

    private void prepareCache(NearCacheConfig nearCacheConfig) {
        hazelcastFactory.newHazelcastInstance(createConfig());

        ClientConfig clientConfig = createClientConfig();
        if (nearCacheConfig != null) {
            clientConfig.addNearCacheConfig(nearCacheConfig);
        }

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();

        CacheConfig<String, SerializationCountingData> cacheConfig = createCacheConfig(InMemoryFormat.BINARY);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();

        cache = cacheManager.createCache(CACHE_NAME, cacheConfig);
        nearCache = nearCacheManager.getNearCache(cacheManager.getCacheNameWithPrefix(CACHE_NAME));
    }


    private void assertAndReset(int serializeCount, int deserializeCount) {
        assertEquals(serializeCount, this.serializeCount.getAndSet(0));
        assertEquals(deserializeCount, this.deserializeCount.getAndSet(0));
    }


}
