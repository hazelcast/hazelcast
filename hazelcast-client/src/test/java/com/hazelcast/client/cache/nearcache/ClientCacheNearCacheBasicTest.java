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

package com.hazelcast.client.cache.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.DataStructureAdapterMethod;
import com.hazelcast.internal.adapter.ICacheCacheLoader;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCacheBasicTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.internal.nearcache.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.NearCacheTestUtils;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_LOCAL_UPDATE_POLICY;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getBaseConfig;

/**
 * Basic Near Cache tests for {@link ICache} on Hazelcast clients.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheNearCacheBasicTest extends AbstractNearCacheBasicTest<Data, String> {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(DEFAULT_MEMORY_FORMAT, DEFAULT_SERIALIZE_KEYS)
                .setLocalUpdatePolicy(DEFAULT_LOCAL_UPDATE_POLICY);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(ICacheDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean loaderEnabled) {
        Config config = getConfig();
        CacheConfig<K, V> cacheConfig = getCacheConfig(nearCacheConfig, loaderEnabled);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        CachingProvider memberProvider = HazelcastServerCachingProvider.createCachingProvider(member);
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) memberProvider.getCacheManager();
        ICache<K, V> memberCache = memberCacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);
        ICacheDataStructureAdapter<K, V> dataAdapter = new ICacheDataStructureAdapter<K, V>(memberCache);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder(cacheConfig);
        return builder
                .setDataInstance(member)
                .setDataAdapter(dataAdapter)
                .setMemberCacheManager(memberCacheManager)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        CacheConfig<K, V> cacheConfig = getCacheConfig(nearCacheConfig, false);
        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder(cacheConfig);
        return builder.build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    @SuppressWarnings("unchecked")
    private <K, V> CacheConfig<K, V> getCacheConfig(NearCacheConfig nearCacheConfig, boolean loaderEnabled) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setName(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(nearCacheConfig.getInMemoryFormat());

        if (nearCacheConfig.getInMemoryFormat() == NATIVE) {
            cacheConfig.getEvictionConfig()
                    .setEvictionPolicy(LRU)
                    .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                    .setSize(90);
        }

        if (loaderEnabled) {
            cacheConfig
                    .setReadThrough(true)
                    .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(ICacheCacheLoader.class));
        }

        return cacheConfig;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder(CacheConfig<K, V> cacheConfig) {
        ClientConfig clientConfig = getClientConfig();

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        ICache<K, V> clientCache = cacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(DEFAULT_NEAR_CACHE_NAME);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheNameWithPrefix);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ICacheDataStructureAdapter<K, V>(clientCache))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setCacheManager(cacheManager);
    }
}
