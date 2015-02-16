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

package com.hazelcast.client.cache.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientNearCacheTest {

    private static final String DEFAULT_CACHE_NAME = "ClientCache";
    private static final int DEFAULT_RECORD_COUNT = 100;

    private HazelcastInstance serverInstance;

    @Before
    public void setup() {
        serverInstance = Hazelcast.newHazelcastInstance();
    }

    @After
    public void tearDown() {
        if (serverInstance != null) {
            serverInstance.shutdown();
        }
    }

    class NearCacheTestContext {

        final HazelcastClientProxy client;
        final SerializationService serializationService;
        final HazelcastClientCacheManager cacheManager;
        final NearCacheManager nearCacheManager;
        final ICache<Integer, String> cache;
        final NearCache<Data, String> nearCache;

        NearCacheTestContext(HazelcastClientProxy client,
                             HazelcastClientCacheManager cacheManager, NearCacheManager nearCacheManager,
                             ICache<Integer, String> cache, NearCache<Data, String> nearCache) {
            this.client = client;
            this.serializationService = client.getSerializationService();
            this.cacheManager = cacheManager;
            this.nearCacheManager = nearCacheManager;
            this.cache = cache;
            this.nearCache = nearCache;
        }

        void close() {
            cache.destroy();
            client.shutdown();
        }

    }

    private String generateValueFromKey(Integer key) {
        return "Value-" + key;
    }

    private NearCacheTestContext getNearCacheTest(String cacheName) {
        HazelcastClientProxy client = (HazelcastClientProxy) HazelcastClient.newHazelcastClient();
        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();

        ICache<Integer, String> cache = cacheManager.getCache(cacheName);

        NearCache<Data, String> nearCache =
                nearCacheManager.getNearCache(cacheManager.getCacheNameWithPrefix(cacheName));

        return new NearCacheTestContext(client, cacheManager, nearCacheManager, cache, nearCache);
    }

    private NearCacheTestContext createNearCacheTest(String cacheName, NearCacheConfig nearCacheConfig) {
        HazelcastClientProxy client = (HazelcastClientProxy) HazelcastClient.newHazelcastClient();
        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();

        CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>();
        cacheConfig.setNearCacheConfig(nearCacheConfig);
        ICache<Integer, String> cache = cacheManager.createCache(cacheName, cacheConfig);

        NearCache<Data, String> nearCache =
                nearCacheManager.getNearCache(cacheManager.getCacheNameWithPrefix(cacheName));

        return new NearCacheTestContext(client, cacheManager, nearCacheManager, cache, nearCache);
    }

    private NearCacheTestContext createNearCacheTestAndFillWithData(String cacheName,
                                                                    NearCacheConfig nearCacheConfig) {
        NearCacheTestContext nearCacheTestContext = createNearCacheTest(cacheName, nearCacheConfig);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext.cache.put(i, generateValueFromKey(i));
        }
        return nearCacheTestContext;
    }

    private void putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(inMemoryFormat);
        final NearCacheTestContext nearCacheTestContext =
                createNearCacheTestAndFillWithData(DEFAULT_CACHE_NAME, nearCacheConfig);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheTestContext.nearCache.get(
                            nearCacheTestContext.serializationService.toData(i)));
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            // Get records so they will be stored in near-cache
            nearCacheTestContext.cache.get(i);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final String expectedValue = generateValueFromKey(i);
            final Data keyData = nearCacheTestContext.serializationService.toData(i);
            // Entries are stored in the near-cache as async not sync.
            // So these records will be there in near-cache eventually.
            HazelcastTestSupport.assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(expectedValue, nearCacheTestContext.nearCache.get(keyData));
                }
            });
        }

        nearCacheTestContext.close();
    }

    @Test
    public void putAndGetFromCacheAndThenGetFromClientNearCacheWithBinaryInMemoryFormat() {
        putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putAndGetFromCacheAndThenGetFromClientNearCacheWithObjectInMemoryFormat() {
        putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat.OBJECT);
    }

    private void putToCacheAndThenGetFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(inMemoryFormat);
        nearCacheConfig.setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE);
        NearCacheTestContext nearCacheTestContext =
                createNearCacheTestAndFillWithData(DEFAULT_CACHE_NAME, nearCacheConfig);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals(generateValueFromKey(i),
                         nearCacheTestContext.nearCache.get(
                                nearCacheTestContext.serializationService.toData(i)));
        }

        nearCacheTestContext.close();
    }

    @Test
    public void putToCacheAndThenGetFromClientNearCacheWithBinaryInMemoryFormat() {
        putToCacheAndThenGetFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putToCacheAndThenGetFromClientNearCacheWithObjectInMemoryFormat() {
        putToCacheAndThenGetFromClientNearCache(InMemoryFormat.OBJECT);
    }

    private void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setInMemoryFormat(inMemoryFormat);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = getNearCacheTest(DEFAULT_CACHE_NAME);

        // Put cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.put(i, generateValueFromKey(i));
        }

        // Get records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final Integer key = i;
            final String value = nearCacheTestContext2.cache.get(key);
            // Records are stored in the cache as async not sync.
            // So these records will be there in cache eventually.
            HazelcastTestSupport.assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(value,
                            nearCacheTestContext2.nearCache.get(
                                    nearCacheTestContext2.serializationService.toData(key)));
                }
            });
        }

        // Update cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.put(i, generateValueFromKey(DEFAULT_RECORD_COUNT + i));
        }

        // Get updated records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final int key = i;
            // Records are stored in the near-cache will be invalidated eventually
            // since cache records are updated.
            HazelcastTestSupport.assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertNull(nearCacheTestContext2.nearCache.get(
                            nearCacheTestContext2.serializationService.toData(key)));
                }
            });
        }

        // Get updated records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final Integer key = i;
            final String value = nearCacheTestContext2.cache.get(key);
            // Records are stored in the cache as async not sync.
            // So these records will be there in cache eventually.
            HazelcastTestSupport.assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(value,
                            nearCacheTestContext2.nearCache.get(
                                    nearCacheTestContext2.serializationService.toData(key)));
                }
            });
        }

        nearCacheTestContext1.close();
        nearCacheTestContext2.close();
    }

    @Test
    public void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCacheWithBinaryInMemoryFormat() {
        putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat.BINARY);
    }

    @Test
    public void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCacheWithObjectInMemoryFormat() {
        putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat.OBJECT);
    }

}
