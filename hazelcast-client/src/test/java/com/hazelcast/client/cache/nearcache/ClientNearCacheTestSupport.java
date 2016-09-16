/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListener;
import javax.cache.spi.CachingProvider;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.properties.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public abstract class ClientNearCacheTestSupport extends HazelcastTestSupport {

    protected static final String DEFAULT_CACHE_NAME = "ClientCache";
    protected static final int DEFAULT_RECORD_COUNT = 100;
    protected static final int MAX_TTL_SECONDS = 2;
    protected static final int MAX_IDLE_SECONDS = 1;

    protected HazelcastInstance serverInstance;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setup() {
        serverInstance = hazelcastFactory.newHazelcastInstance(createConfig());
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    protected Config createConfig() {
        return new Config();
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = new CacheConfig().setName(DEFAULT_CACHE_NAME).setInMemoryFormat(inMemoryFormat);
        //noinspection unchecked
        cacheConfig.setCacheLoaderFactory(FactoryBuilder.factoryOf(ClientNearCacheTestSupport.TestCacheLoader.class));
        return cacheConfig;
    }

    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected String generateValueFromKey(Integer key) {
        return "Value-" + key;
    }

    protected NearCacheTestContext createNearCacheTest(String cacheName, NearCacheConfig nearCacheConfig,
                                                       CacheConfig cacheConfig) {
        ClientConfig clientConfig = createClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();

        //noinspection unchecked
        ICache<Object, String> cache = cacheManager.createCache(cacheName, cacheConfig);

        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheManager.getCacheNameWithPrefix(cacheName));

        return new NearCacheTestContext(client, cacheManager, nearCacheManager, cache, nearCache);
    }

    protected NearCacheTestContext createNearCacheTest(String cacheName, NearCacheConfig nearCacheConfig) {
        CacheConfig cacheConfig = createCacheConfig(nearCacheConfig.getInMemoryFormat());
        return createNearCacheTest(cacheName, nearCacheConfig, cacheConfig);
    }

    protected NearCacheTestContext createNearCacheTestAndFillWithData(String cacheName, NearCacheConfig nearCacheConfig) {
        return createNearCacheTestAndFillWithData(cacheName, nearCacheConfig, false);
    }

    protected NearCacheTestContext createNearCacheTestAndFillWithData(String cacheName, NearCacheConfig nearCacheConfig,
                                                                      boolean putIfAbsent) {
        NearCacheTestContext nearCacheTestContext = createNearCacheTest(cacheName, nearCacheConfig);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            if (putIfAbsent) {
                nearCacheTestContext.cache.putIfAbsent(i, generateValueFromKey(i));
            } else {
                nearCacheTestContext.cache.put(i, generateValueFromKey(i));
            }
        }
        return nearCacheTestContext;
    }

    protected void whenEmptyMapThenPopulatedNearCacheShouldReturnNullNeverNULL_OBJECT(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        NearCacheTestContext nearCacheTestContext = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            // populate Near Cache
            assertNull(nearCacheTestContext.cache.get(i));
            // fetch value from Near Cache
            assertNull(nearCacheTestContext.cache.get(i));
        }
    }

    protected void putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        NearCacheTestContext nearCacheTestContext = createNearCacheTestAndFillWithData(DEFAULT_CACHE_NAME, nearCacheConfig);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheTestContext.nearCache.get(nearCacheTestContext.serializationService.toData(i)));
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            // get records so they will be stored in Near Cache
            nearCacheTestContext.cache.get(i);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String expectedValue = generateValueFromKey(i);
            Data keyData = nearCacheTestContext.serializationService.toData(i);
            assertEquals(expectedValue, nearCacheTestContext.nearCache.get(keyData));
        }
    }

    protected void putToCacheAndThenGetFromClientNearCacheInternal(InMemoryFormat inMemoryFormat, boolean putIfAbsent) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setLocalUpdatePolicy(LocalUpdatePolicy.CACHE);
        NearCacheTestContext nearCacheTestContext = createNearCacheTestAndFillWithData(DEFAULT_CACHE_NAME, nearCacheConfig,
                putIfAbsent);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String expectedValue = generateValueFromKey(i);
            Data keyData = nearCacheTestContext.serializationService.toData(i);
            assertEquals(expectedValue, nearCacheTestContext.nearCache.get(keyData));
        }
    }

    protected void putToCacheAndThenGetFromClientNearCache(InMemoryFormat inMemoryFormat) {
        putToCacheAndThenGetFromClientNearCacheInternal(inMemoryFormat, false);
    }

    protected void putIfAbsentToCacheAndThenGetFromClientNearCache(InMemoryFormat inMemoryFormat) {
        putToCacheAndThenGetFromClientNearCacheInternal(inMemoryFormat, true);
    }

    protected void putAsyncToCacheAndThenGetFromClientNearCacheImmediately(InMemoryFormat inMemoryFormat) throws Exception {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setLocalUpdatePolicy(LocalUpdatePolicy.CACHE);
        NearCacheTestContext nearCacheTestContext = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        for (int i = 0; i < 10 * DEFAULT_RECORD_COUNT; i++) {
            String expectedValue = generateValueFromKey(i);
            Data keyData = nearCacheTestContext.serializationService.toData(i);
            Future future = nearCacheTestContext.cache.putAsync(i, expectedValue);
            future.get();
            assertEquals(expectedValue, nearCacheTestContext.nearCache.get(keyData));
        }
    }

    protected void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        // put cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.put(i, generateValueFromKey(i));
        }

        // get records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final Integer key = i;
            final String value = nearCacheTestContext2.cache.get(key);
            // records are stored in the cache as async not sync, so these records will be there in cache eventually
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertEquals(value, nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }

        // update cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.put(i, generateValueFromKey(DEFAULT_RECORD_COUNT + i));
        }

        // get updated records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final int key = i;
            // records are stored in the Near Cache will be invalidated eventually, since cache records are updated
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertNull(nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }

        // get updated records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final Integer key = i;
            final String value = nearCacheTestContext2.cache.get(key);
            // records are stored in the cache as async not sync, so these records will be there in cache eventually
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertEquals(value, nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }
    }

    protected void putToCacheAndGetInvalidationEventWhenNodeShutdown(InMemoryFormat inMemoryFormat) {
        Config config = createConfig();
        config.setProperty(GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(Integer.MAX_VALUE));
        config.setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));
        HazelcastInstance instanceToShutdown = hazelcastFactory.newHazelcastInstance(config);

        warmUpPartitions(serverInstance, instanceToShutdown);
        waitAllForSafeState(serverInstance, instanceToShutdown);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setLocalUpdatePolicy(LocalUpdatePolicy.CACHE);
        final NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        Map<String, String> keyAndValues = new HashMap<String, String>();

        // put cache record from client-1 to instance which is going to be shutdown
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String key = generateKeyOwnedBy(instanceToShutdown);
            String value = generateValueFromKey(i);
            nearCacheTestContext1.cache.put(key, value);
            keyAndValues.put(key, value);
        }

        // verify that records are exist at Near Cache of client-1 because `local-update-policy` is `CACHE`
        for (Map.Entry<String, String> entry : keyAndValues.entrySet()) {
            String key = entry.getKey();
            String exceptedValue = entry.getValue();
            String actualValue = nearCacheTestContext1.nearCache.get(nearCacheTestContext1.serializationService.toData(key));
            assertEquals(exceptedValue, actualValue);
        }

        // remove records through client-2 so there will be invalidation events
        // to send to client to invalidate its Near Cache
        for (Map.Entry<String, String> entry : keyAndValues.entrySet()) {
            nearCacheTestContext2.cache.remove(entry.getKey());
        }

        // we don't shutdown the instance because in case of shutdown even though events are published to event queue,
        // they may not be processed in the event queue due to shutdown event queue executor or may not be sent
        // to client endpoint due to IO handler shutdown

        // for not to making test fragile, we just simulate shutting down by sending its event through `LifeCycleService`,
        // so the node should flush invalidation events before shutdown
        ((LifecycleServiceImpl) instanceToShutdown.getLifecycleService())
                .fireLifecycleEvent(LifecycleEvent.LifecycleState.SHUTTING_DOWN);

        // verify that records in the Near Cache of client-1 are invalidated eventually when instance shutdown
        for (Map.Entry<String, String> entry : keyAndValues.entrySet()) {
            final String key = entry.getKey();
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext1.serializationService.toData(key);
                    assertNull(nearCacheTestContext1.nearCache.get(keyData));
                }
            });
        }
    }

    protected void putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        // put cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.put(i, generateValueFromKey(i));
        }

        // get records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final Integer key = i;
            final String value = nearCacheTestContext2.cache.get(key);
            // records are stored in the cache as async not sync, so these records will be there in cache eventually
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertEquals(value, nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }

        // delete cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.remove(i);
        }

        // can't get deleted records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final int key = i;
            // records are stored in the Near Cache will be invalidated eventually, since cache records are updated.
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertNull(nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }
    }

    protected void testLoadAllNearCacheInvalidation(InMemoryFormat inMemoryFormat) throws Exception {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        Set<Integer> testKeys = new HashSet<Integer>(DEFAULT_RECORD_COUNT);
        Set<Integer> loadKeys = new HashSet<Integer>(DEFAULT_RECORD_COUNT / 2);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            if (i % 2 == 0) {
                loadKeys.add(i);
            }
            testKeys.add(i);
        }

        // populate cache from client-1
        for (int i : testKeys) {
            nearCacheTestContext1.cache.put(i, generateValueFromKey(i));
        }

        final CountDownLatch completed = new CountDownLatch(1);
        nearCacheTestContext1.cache.loadAll(loadKeys, true, new CompletionListener() {
            @Override
            public void onCompletion() {
                completed.countDown();
            }

            @Override
            public void onException(Exception e) {
            }
        });

        completed.await(3, TimeUnit.SECONDS);

        // can't get replaced keys from client-2
        for (int i : loadKeys) {
            final int key = i;
            // records are stored in the Near Cache will be invalidated eventually, since cache records are updated
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertNull(nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }
    }

    protected void putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCache(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        // put cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.put(i, generateValueFromKey(i));
        }

        // get records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final Integer key = i;
            final String value = nearCacheTestContext2.cache.get(key);
            // records are stored in the cache as async not sync, so these records will be there in cache eventually
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertEquals(value, nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }

        nearCacheTestContext1.cache.clear();

        // can't get expired records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final int key = i;
            // records are stored in the Near Cache will be invalidated eventually, since cache records are cleared
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertNull(nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }
    }

    protected void doTestGetAllReturnsFromNearCache(InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat != InMemoryFormat.OBJECT) {
            return;
        }
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        NearCacheTestContext nearCacheTestContext = createNearCacheTestAndFillWithData(DEFAULT_CACHE_NAME, nearCacheConfig);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheTestContext.nearCache.get(nearCacheTestContext.serializationService.toData(i)));
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            // get records so they will be stored in Near Cache
            nearCacheTestContext.cache.get(i);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            Data keyData = nearCacheTestContext.serializationService.toData(i);
            // check if same reference to verify data coming from Near Cache
            assertSame(nearCacheTestContext.cache.get(i), nearCacheTestContext.nearCache.get(keyData));
        }
    }

    protected void putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabled(InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = createCacheConfig(inMemoryFormat);
        cacheConfig.setDisablePerEntryInvalidationEvents(true);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext nearCacheTestContext1 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig, cacheConfig);
        final NearCacheTestContext nearCacheTestContext2 = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig, cacheConfig);

        // put cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheTestContext1.cache.put(i, generateValueFromKey(i));
        }

        // get records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final Integer key = i;
            final String value = nearCacheTestContext2.cache.get(key);
            // records are stored in the cache as async not sync, so these records will be there in cache eventually
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertEquals(value, nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }

        // update cache record from client-1
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            // update the cache records with new values
            nearCacheTestContext1.cache.put(i, generateValueFromKey(i + DEFAULT_RECORD_COUNT));
        }

        int invalidationEventFlushFreq = Integer.parseInt(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getDefaultValue());
        // wait some time and if there are invalidation events to be sent in batch
        // (we assume that they should be flushed, received and processed in this time window already)
        sleepSeconds(2 * invalidationEventFlushFreq);

        // get records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String actualValue = nearCacheTestContext2.cache.get(i);
            String expectedValue = generateValueFromKey(i);
            // verify that still we have old records in the Near Cache, because, per entry invalidation events are disabled
            assertEquals(expectedValue, actualValue);
        }

        nearCacheTestContext1.cache.clear();

        // can't get expired records from client-2
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            final int key = i;
            // records are stored in the Near Cache will be invalidated eventually, since cache records are cleared
            // because we just disable per entry invalidation events, not full-flush events
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Data keyData = nearCacheTestContext2.serializationService.toData(key);
                    assertNull(nearCacheTestContext2.nearCache.get(keyData));
                }
            });
        }
    }

    protected void testNearCacheEviction(InMemoryFormat inMemoryFormat) {
        int size = 100;
        int expectedEvictions = 1;

        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT)
                .setSize(size);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setEvictionConfig(evictionConfig);

        NearCacheTestContext context = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        // populate map with an extra entry
        for (int i = 0; i < size + 1; i++) {
            context.cache.put(i, "value-" + i);
        }

        // populate Near Caches
        for (int i = 0; i < size; i++) {
            context.cache.get(i);
        }

        NearCacheStats statsBeforeEviction = getNearCacheStats(context.cache);

        // trigger eviction via fetching the extra entry
        context.cache.get(size);

        waitForNearCacheEvictions(context.cache, expectedEvictions);

        // we expect (size + the extra entry - the expectedEvictions) entries in the Near Cache
        int expectedOwnedEntryCount = size + 1 - expectedEvictions;

        NearCacheStats stats = getNearCacheStats(context.cache);
        assertEquals("got the wrong ownedEntryCount", expectedOwnedEntryCount, stats.getOwnedEntryCount());
        assertEquals("got the wrong eviction count", expectedEvictions, stats.getEvictions());
        assertEquals("got the wrong expiration count", 0, stats.getExpirations());
        assertEquals("we expect the same hits", statsBeforeEviction.getHits(), stats.getHits());
        assertEquals("we expect the same misses", statsBeforeEviction.getMisses(), stats.getMisses());
    }

    protected void testNearCacheExpiration_withTTL(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setTimeToLiveSeconds(MAX_TTL_SECONDS);

        testNearCacheExpiration(nearCacheConfig, MAX_TTL_SECONDS);
    }

    protected void testNearCacheExpiration_withMaxIdle(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setTimeToLiveSeconds(MAX_IDLE_SECONDS);

        testNearCacheExpiration(nearCacheConfig, MAX_IDLE_SECONDS);
    }

    private void testNearCacheExpiration(NearCacheConfig nearCacheConfig, int expireSeconds) {
        final int size = 147;

        final NearCacheTestContext context = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        for (int i = 0; i < size; i++) {
            context.cache.put(i, "value-" + i);
            context.cache.get(i);
        }

        final NearCacheStats statsBeforeExpiration = getNearCacheStats(context.cache);
        assertEquals(format("we expected to have all map entries in the Near Cache (%s)", statsBeforeExpiration),
                size, statsBeforeExpiration.getOwnedEntryCount() + statsBeforeExpiration.getExpirations());

        sleepSeconds(expireSeconds + 1);

        assertTrueEventually(new AssertTask() {
            public void run() {
                // map.get() triggers Near Cache eviction/expiration process,
                // but we need to call this on every assert since the Near Cache has a cooldown for TTL cleanups
                context.cache.get(0);

                NearCacheStats stats = getNearCacheStats(context.cache);
                assertEquals("we expect the same hits", statsBeforeExpiration.getHits(), stats.getHits());
                assertEquals("we expect the same misses", statsBeforeExpiration.getMisses(), stats.getMisses());
                assertEquals("we expect all entries beside the 'trigger entry' to be deleted from the Near Cache",
                        1, stats.getOwnedEntryCount());
                assertEquals("we did not expect any entries to be evicted from the Near Cache",
                        0, stats.getEvictions());
                assertTrue(format("we expect at least %d entries to be expired from the Near Cache", size),
                        stats.getExpirations() >= size);
            }
        });
    }

    protected void testNearCacheMemoryCostCalculation(InMemoryFormat inMemoryFormat, int threadCount) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat).setCacheLocalEntries(true);
        final NearCacheTestContext context = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            context.cache.put(i, "value-" + i);
        }

        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        Runnable task = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                    context.cache.get(i);
                }
                countDownLatch.countDown();
            }
        };

        ExecutorService executorService = newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(task);
        }
        assertOpenEventually(countDownLatch);

        // the Near Cache is filled, we should see some memory costs now
        long memoryCosts = context.cache.getLocalCacheStatistics().getNearCacheStatistics().getOwnedEntryMemoryCost();
        if (inMemoryFormat == InMemoryFormat.OBJECT) {
            assertEquals("There should be no Near Cache heap costs calculated for InMemoryFormat.OBJECT", 0, memoryCosts);
        } else {
            assertTrue("The Near Cache is filled, there should be some heap costs", memoryCosts > 0);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            context.cache.remove(i);
        }

        // the Near Cache is empty, we shouldn't see memory costs anymore
        assertEquals("The Near Cache is empty, there should be no heap costs", 0,
                context.cache.getLocalCacheStatistics().getNearCacheStatistics().getOwnedEntryMemoryCost());
    }

    protected NearCacheStats getNearCacheStats(ICache cache) {
        return cache.getLocalCacheStatistics().getNearCacheStatistics();
    }

    protected void waitForNearCacheEvictions(final ICache cache, final int evictionCount) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                long evictions = getNearCacheStats(cache).getEvictions();
                assertTrue(
                        format("Near Cache eviction count didn't reach the desired value (%d vs. %d)", evictions, evictionCount),
                        evictions >= evictionCount);
            }
        });
    }

    public static class TestCacheLoader implements CacheLoader<Integer, String> {

        @Override
        public String load(Integer key) throws CacheLoaderException {
            return String.valueOf(2 * key);
        }

        @Override
        public Map<Integer, String> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            Map<Integer, String> entries = new HashMap<Integer, String>();
            for (int key : keys) {
                entries.put(key, String.valueOf(2 * key));
            }
            return entries;
        }
    }
}
