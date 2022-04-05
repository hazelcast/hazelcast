/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.AbstractNearCacheLeakTest;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContext;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.spi.CachingProvider;
import java.util.Collection;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static java.util.Arrays.asList;

/**
 * Basic Near Cache tests for {@link ICache} on Hazelcast clients.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheLeakTest extends AbstractNearCacheLeakTest<Data, String> {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean serializeKeys;

    @Parameter(value = 2)
    public LocalUpdatePolicy localUpdatePolicy;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "format:{0} serializeKeys:{1} localUpdatePolicy:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.BINARY, true, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.BINARY, false, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.BINARY, false, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.OBJECT, true, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.OBJECT, true, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.OBJECT, false, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.OBJECT, false, LocalUpdatePolicy.CACHE_ON_UPDATE},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(inMemoryFormat, serializeKeys)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(localUpdatePolicy);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig();
        CacheConfig<K, V> cacheConfig = getCacheConfig(nearCacheConfig);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        CachingProvider memberProvider = createServerCachingProvider(member);
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
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    @SuppressWarnings("unchecked")
    private <K, V> CacheConfig<K, V> getCacheConfig(NearCacheConfig nearCacheConfig) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setName(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(nearCacheConfig.getInMemoryFormat());

        if (nearCacheConfig.getInMemoryFormat() == NATIVE) {
            cacheConfig.getEvictionConfig()
                    .setEvictionPolicy(LRU)
                    .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                    .setSize(90);
        }

        return cacheConfig;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder(CacheConfig<K, V> cacheConfig) {
        ClientConfig clientConfig = getClientConfig();

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider provider = createClientCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        ICache<K, V> clientCache = cacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        NearCacheManager nearCacheManager = ((ClientCacheProxy) clientCache).getContext()
                .getNearCacheManager(clientCache.getServiceName());
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(DEFAULT_NEAR_CACHE_NAME);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheNameWithPrefix);

        ClientContext clientContext = ((ClientProxy) clientCache).getContext();
        RepairingTask repairingTask = clientContext.getRepairingTask(CacheService.SERVICE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ICacheDataStructureAdapter<K, V>(clientCache))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setCacheManager(cacheManager)
                .setRepairingTask(repairingTask);
    }
}
