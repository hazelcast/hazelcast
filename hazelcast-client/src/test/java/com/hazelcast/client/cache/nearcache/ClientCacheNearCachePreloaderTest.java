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
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCachePreloaderTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.internal.nearcache.NearCacheTestContextBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.spi.CachingProvider;
import java.io.File;

import static com.hazelcast.cache.CacheUtil.getDistributedObjectName;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_INVALIDATE_ON_CHANGE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.nio.IOUtil.toFileName;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("WeakerAccess")
public class ClientCacheNearCachePreloaderTest extends AbstractNearCachePreloaderTest<Data, String> {

    protected final String cacheFileName = toFileName(getDistributedObjectName(defaultNearCache));
    protected final File storeFile = new File("nearCache-" + cacheFileName + ".store").getAbsoluteFile();
    protected final File storeLockFile = new File(storeFile.getName() + ".lock").getAbsoluteFile();

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(DEFAULT_MEMORY_FORMAT, DEFAULT_SERIALIZE_KEYS, DEFAULT_INVALIDATE_ON_CHANGE,
                KEY_COUNT, storeFile.getParent());
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected File getStoreFile() {
        return storeFile;
    }

    @Override
    protected File getStoreLockFile() {
        return storeLockFile;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <K, V> DataStructureAdapter<K, V> getDataStructure(NearCacheTestContext<K, V, Data, String> context, String name) {
        CacheConfig<K, V> cacheConfig = getCacheConfig(nearCacheConfig);

        Cache<K, V> memberCache = context.cacheManager.createCache(name, cacheConfig.setName(name));
        return new ICacheDataStructureAdapter<K, V>((ICache<K, V>) memberCache.unwrap(ICache.class));
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean createNearCacheInstance) {
        Config config = getConfig();

        CacheConfig<K, V> cacheConfig = getCacheConfig(nearCacheConfig);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        CachingProvider memberProvider = HazelcastServerCachingProvider.createCachingProvider(member);
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) memberProvider.getCacheManager();
        ICache<K, V> memberCache = memberCacheManager.createCache(nearCacheConfig.getName(), cacheConfig);
        ICacheDataStructureAdapter<K, V> dataAdapter = new ICacheDataStructureAdapter<K, V>(memberCache);

        if (createNearCacheInstance) {
            return createNearCacheContextBuilder(cacheConfig)
                    .setDataInstance(member)
                    .setDataAdapter(dataAdapter)
                    .setMemberCacheManager(memberCacheManager)
                    .build();
        }
        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(member))
                .setDataInstance(member)
                .setDataAdapter(dataAdapter)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        CacheConfig<K, V> cacheConfig = getCacheConfig(nearCacheConfig);
        return createNearCacheContextBuilder(cacheConfig).build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> CacheConfig<K, V> getCacheConfig(NearCacheConfig nearCacheConfig) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setName(nearCacheConfig.getName())
                .setInMemoryFormat(nearCacheConfig.getInMemoryFormat());

        if (nearCacheConfig.getInMemoryFormat() == NATIVE) {
            cacheConfig.getEvictionConfig()
                    .setEvictionPolicy(LRU)
                    .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                    .setSize(90);
        }

        return cacheConfig;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder(CacheConfig<K, V> cacheConfig) {
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(nearCacheConfig.getName());
        ICache<K, V> clientCache = cacheManager.createCache(nearCacheConfig.getName(), cacheConfig);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheNameWithPrefix);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ICacheDataStructureAdapter<K, V>(clientCache))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setCacheManager(cacheManager);
    }
}
