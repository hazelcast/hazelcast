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

package com.hazelcast.client.cache;

import com.hazelcast.cache.CacheReadWriteThroughTest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.spi.CachingProvider;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheReadWriteThroughTest extends CacheReadWriteThroughTest {

    private static final String CACHE_WITH_NEARCACHE = randomName();
    private static final int NEARCACHE_SIZE = 100;

    private CachingProvider serverCachingProvider;

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return createClientCachingProvider(instance);
    }

    @Override
    protected TestHazelcastInstanceFactory createInstanceFactory(int instanceCount) {
        return new TestHazelcastFactory();
    }

    @Override
    protected HazelcastInstance getInstance() {
        // Create server instance
        HazelcastInstance serverInstance = factory.newHazelcastInstance(createConfig());
        serverCachingProvider = createServerCachingProvider(serverInstance);
        // Create client instance
        ClientConfig clientConfig = new ClientConfig();
        NearCacheConfig nearCacheConfig = new NearCacheConfig(CACHE_WITH_NEARCACHE);
        nearCacheConfig.getEvictionConfig().setSize(NEARCACHE_SIZE);
        clientConfig.addNearCacheConfig(nearCacheConfig);
        return ((TestHazelcastFactory) factory).newHazelcastClient(clientConfig);
    }

    @Override
    protected void onTearDown() {
        serverCachingProvider.close();
    }

    // https://github.com/hazelcast/hazelcast/issues/6676
    @Test
    public void test_cacheLoaderIsUsedOnlyAtServerSide() {
        final String cacheName = randomName();
        CacheManager serverCacheManager = serverCachingProvider.getCacheManager();

        CompleteConfiguration<Integer, String> config =
                new CacheConfig<Integer, String>()
                        .setTypes(Integer.class, String.class)
                        .setReadThrough(true)
                        .setCacheLoaderFactory(new ServerSideCacheLoaderFactory());

        serverCacheManager.createCache(cacheName, config);

        CacheManager clientCacheManager = cachingProvider.getCacheManager();
        Cache<Integer, String> cache = clientCacheManager.getCache(cacheName, Integer.class, String.class);

        assertNotNull(cache);

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            keys.add(i);
        }

        Map<Integer, String> loaded = cache.getAll(keys);
        assertEquals(keys.size(), loaded.size());
        for (Map.Entry<Integer, String> entry : loaded.entrySet()) {
            assertEquals(ServerSideCacheLoader.valueOf(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void test_readThroughCacheLoader_withNearCache() {
        String cacheName = CACHE_WITH_NEARCACHE;
        CacheConfiguration<Integer, String> cacheConfig =
                new CacheConfig<Integer, String>()
                        .setReadThrough(true)
                        .setCacheLoaderFactory(new ServerSideCacheLoaderFactory());

        serverCachingProvider.getCacheManager().createCache(cacheName, cacheConfig);

        Cache<Integer, String> cache = cachingProvider.getCacheManager().getCache(cacheName);

        for (int i = 0; i < NEARCACHE_SIZE * 5; i++) {
            assertNotNull(cache.get(i));
        }
    }

    public static class ServerSideCacheLoaderFactory
            implements Factory<ServerSideCacheLoader>, HazelcastInstanceAware {

        private transient HazelcastInstance hazelcastInstance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public ServerSideCacheLoader create() {
            if (hazelcastInstance instanceof HazelcastInstanceImpl) {
                return new ServerSideCacheLoader();
            } else {
                throw new IllegalStateException("This factory can only be used at server side!");
            }
        }
    }

    private static class ServerSideCacheLoader implements CacheLoader<Integer, String> {

        static String valueOf(Integer key) {
            return "value-of-" + key;
        }

        @Override
        public String load(Integer key) {
            return valueOf(key);
        }

        @Override
        public Map<Integer, String> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            Map<Integer, String> result = new HashMap<Integer, String>();
            for (Integer key : keys) {
                String value = load(key);
                result.put(key, value);
            }
            return result;
        }
    }
}
