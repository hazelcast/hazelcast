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

package com.hazelcast.client.cache.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCacheSerializationCountTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.spi.CachingProvider;
import java.util.Collection;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link ICache} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheNearCacheSerializationCountTest extends AbstractNearCacheSerializationCountTest<Data, String> {

    @Parameter
    public int[] expectedSerializationCounts;

    @Parameter(value = 1)
    public int[] expectedDeserializationCounts;

    @Parameter(value = 2)
    public InMemoryFormat cacheInMemoryFormat;

    @Parameter(value = 3)
    public InMemoryFormat nearCacheInMemoryFormat;

    @Parameter(value = 4)
    public LocalUpdatePolicy localUpdatePolicy;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "cacheFormat:{2} nearCacheFormat:{3} localUpdatePolicy:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, null, null,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, BINARY, LocalUpdatePolicy.INVALIDATE,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, BINARY, LocalUpdatePolicy.CACHE_ON_UPDATE,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 0}, BINARY, OBJECT, LocalUpdatePolicy.INVALIDATE,},
                {new int[]{1, 0, 0}, new int[]{0, 0, 0}, BINARY, OBJECT, LocalUpdatePolicy.CACHE_ON_UPDATE,},

                {new int[]{1, 1, 1}, new int[]{1, 1, 1}, OBJECT, null, null,},
                {new int[]{1, 1, 0}, new int[]{1, 1, 1}, OBJECT, BINARY, LocalUpdatePolicy.INVALIDATE,},
                {new int[]{1, 0, 0}, new int[]{1, 1, 1}, OBJECT, BINARY, LocalUpdatePolicy.CACHE_ON_UPDATE,},
                {new int[]{1, 1, 0}, new int[]{1, 1, 0}, OBJECT, OBJECT, LocalUpdatePolicy.INVALIDATE,},
                {new int[]{1, 0, 0}, new int[]{1, 0, 0}, OBJECT, OBJECT, LocalUpdatePolicy.CACHE_ON_UPDATE,},
        });
    }

    @Before
    public void setUp() {
        if (nearCacheInMemoryFormat != null) {
            nearCacheConfig = createNearCacheConfig(nearCacheInMemoryFormat)
                    .setLocalUpdatePolicy(localUpdatePolicy);
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected int[] getExpectedSerializationCounts() {
        return expectedSerializationCounts;
    }

    @Override
    protected int[] getExpectedDeserializationCounts() {
        return expectedDeserializationCounts;
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig();
        prepareSerializationConfig(config.getSerializationConfig());

        ClientConfig clientConfig = getClientConfig();
        if (nearCacheConfig != null) {
            clientConfig.addNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(clientConfig.getSerializationConfig());

        CacheConfig<K, V> cacheConfig = createCacheConfig(cacheInMemoryFormat);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        CachingProvider memberProvider = HazelcastServerCachingProvider.createCachingProvider(member);
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) memberProvider.getCacheManager();

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(DEFAULT_NEAR_CACHE_NAME);

        ICache<K, V> clientCache = cacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);
        ICache<K, V> memberCache = memberCacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheNameWithPrefix);

        return new NearCacheTestContext<K, V, Data, String>(
                client.getSerializationService(),
                client,
                member,
                new ICacheDataStructureAdapter<K, V>(clientCache),
                new ICacheDataStructureAdapter<K, V>(memberCache),
                nearCacheConfig,
                false,
                nearCache,
                nearCacheManager,
                cacheManager,
                memberCacheManager
        );
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> CacheConfig<K, V> createCacheConfig(InMemoryFormat inMemoryFormat) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setName(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(0)
                .setAsyncBackupCount(0);

        if (inMemoryFormat == NATIVE) {
            cacheConfig.getEvictionConfig()
                    .setEvictionPolicy(LRU)
                    .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                    .setSize(90);
        }

        return cacheConfig;
    }
}
