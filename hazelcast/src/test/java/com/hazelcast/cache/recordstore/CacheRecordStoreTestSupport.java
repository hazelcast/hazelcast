/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.recordstore;

import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.assertTrue;

public abstract class CacheRecordStoreTestSupport
        extends HazelcastTestSupport {

    protected static final String CACHE_NAME_PREFIX = "hz:";
    protected static final String DEFAULT_CACHE_NAME = "MyCache";
    protected static final int DEFAULT_PARTITION_ID = 1;
    protected static final int CACHE_RECORD_COUNT = 50;

    protected TestHazelcastInstanceFactory factory;
    protected HazelcastInstance hz;

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(1);
        hz = factory.newHazelcastInstance(createConfig());
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    protected Config createConfig() {
        return new Config();
    }

    protected CacheConfig createCacheConfig(String cacheName, InMemoryFormat inMemoryFormat) {
        return new CacheConfig()
                .setName(cacheName)
                .setManagerPrefix(CACHE_NAME_PREFIX)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected ICacheService getCacheService(HazelcastInstance instance) {
        Node node = TestUtil.getNode(instance);
        return node.getNodeEngine().getService(ICacheService.SERVICE_NAME);
    }

    protected NodeEngine getNodeEngine(HazelcastInstance instance) {
        Node node = TestUtil.getNode(instance);
        return node.getNodeEngine();
    }

    protected ICacheRecordStore createCacheRecordStore(HazelcastInstance instance, String cacheName,
                                                       int partitionId, InMemoryFormat inMemoryFormat) {
        NodeEngine nodeEngine = getNodeEngine(instance);
        ICacheService cacheService = getCacheService(instance);
        CacheConfig cacheConfig = createCacheConfig(cacheName, inMemoryFormat);
        cacheService.putCacheConfigIfAbsent(cacheConfig);
        return new CacheRecordStore(CACHE_NAME_PREFIX + cacheName, partitionId, nodeEngine,
                (AbstractCacheService) cacheService);
    }

    protected ICacheRecordStore createCacheRecordStore(HazelcastInstance instance, InMemoryFormat inMemoryFormat) {
        return createCacheRecordStore(instance, DEFAULT_CACHE_NAME, DEFAULT_PARTITION_ID, inMemoryFormat);
    }

    protected ICacheRecordStore createCacheRecordStore(InMemoryFormat inMemoryFormat) {
        return createCacheRecordStore(hz, DEFAULT_CACHE_NAME, DEFAULT_PARTITION_ID, inMemoryFormat);
    }

    protected void putAndGetFromCacheRecordStore(ICacheRecordStore cacheRecordStore, InMemoryFormat inMemoryFormat) {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
            cacheRecordStore.put(serializationService.toData(i), "value-" + i, null, null, -1);
        }

        if (inMemoryFormat == InMemoryFormat.BINARY || inMemoryFormat == InMemoryFormat.NATIVE) {
            for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
                assertTrue(Data.class.isAssignableFrom(
                        cacheRecordStore.get(serializationService.toData(i), null).getClass()));
            }
        } else if (inMemoryFormat == InMemoryFormat.OBJECT) {
            for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
                assertTrue(String.class.isAssignableFrom(
                        cacheRecordStore.get(serializationService.toData(i), null).getClass()));
            }
        } else {
            throw new IllegalArgumentException("Unsupported in-memory format: " + inMemoryFormat);
        }
    }

}
