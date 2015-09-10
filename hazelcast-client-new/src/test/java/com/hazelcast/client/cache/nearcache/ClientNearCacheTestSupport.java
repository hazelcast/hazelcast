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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import javax.cache.spi.CachingProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class ClientNearCacheTestSupport extends HazelcastTestSupport {

    protected static final String DEFAULT_CACHE_NAME = "ClientCache";
    protected static final int DEFAULT_RECORD_COUNT = 100;

    protected HazelcastInstance serverInstance;
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setup() {
        serverInstance = hazelcastFactory.newHazelcastInstance(createConfig());
    }

    @After
    public void tearDown() {
        if (serverInstance != null) {
            serverInstance.shutdown();
        }
    }

    protected Config createConfig() {
        return new Config();
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        return new CacheConfig()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected class NearCacheTestContext {

        protected final HazelcastClientProxy client;
        protected final SerializationService serializationService;
        protected final HazelcastClientCacheManager cacheManager;
        protected final NearCacheManager nearCacheManager;
        protected final ICache<Integer, String> cache;
        protected final NearCache<Data, String> nearCache;

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

    protected String generateValueFromKey(Integer key) {
        return "Value-" + key;
    }

    protected NearCacheTestContext createNearCacheTest(String cacheName, NearCacheConfig nearCacheConfig) {
        ClientConfig clientConfig = createClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();

        CacheConfig<Integer, String> cacheConfig = createCacheConfig(nearCacheConfig.getInMemoryFormat());
        ICache<Integer, String> cache = cacheManager.createCache(cacheName, cacheConfig);

        NearCache<Data, String> nearCache =
                nearCacheManager.getNearCache(cacheManager.getCacheNameWithPrefix(cacheName));

        return new NearCacheTestContext(client, cacheManager, nearCacheManager, cache, nearCache);
    }

    protected NearCacheTestContext createNearCacheTestAndFillWithData(String cacheName,
                                                                      NearCacheConfig nearCacheConfig) {
        NearCacheTestContext nearCacheTestContext = createNearCacheTest(cacheName, nearCacheConfig);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext.cache.put(i, generateValueFromKey(i));
        }
        return nearCacheTestContext;
    }

    protected void putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
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

    protected void putToCacheAndThenGetFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
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

    protected void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

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

    protected void putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

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

        // Delete cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.remove(i);
        }

        // Can't get deleted records from client-2
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

        nearCacheTestContext1.close();
        nearCacheTestContext2.close();
    }

    protected void putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

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

        nearCacheTestContext1.cache.clear();

        // Can't get expired records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final int key = i;
            // Records are stored in the near-cache will be invalidated eventually
            // since cache records are cleared.
            HazelcastTestSupport.assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertNull(nearCacheTestContext2.nearCache.get(
                            nearCacheTestContext2.serializationService.toData(key)));
                }
            });
        }

        nearCacheTestContext1.close();
        nearCacheTestContext2.close();
    }

    protected void doTestGetAllReturnsFromNearCache() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(InMemoryFormat.OBJECT);
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
            final Data keyData = nearCacheTestContext.serializationService.toData(i);
            //check if same reference to verify data coming from near cache
            assertTrue(nearCacheTestContext.cache.get(i) == nearCacheTestContext.nearCache.get(keyData));
        }

        nearCacheTestContext.close();
    }

}
