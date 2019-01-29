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

package com.hazelcast.client.cache.impl.nearcache.invalidation;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.cache.impl.NearCachedClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheNearCacheSmokeTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "test";
    private static final int CLUSTER_SIZE = 3;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final ClientConfig clientConfig = new ClientConfig();
    private final CacheConfig cacheConfig = new CacheConfig();

    private Cache<Integer, Integer> serverCache1;
    private Cache<Integer, Integer> clientCache;

    HazelcastInstance server1;

    @Before
    public void setUp() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);

        clientConfig.addNearCacheConfig(nearCacheConfig);

        cacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        Config config = getConfig();
        config.setProperty(GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "false");

        server1 = factory.newHazelcastInstance(config);
        HazelcastInstance server2 = factory.newHazelcastInstance(config);
        HazelcastInstance server3 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(CLUSTER_SIZE, server1, server2, server3);

        serverCache1 = createServerCache(server1);
    }

    protected NearCacheConfig newNearCacheConfig() {
        return new NearCacheConfig(CACHE_NAME);
    }

    private Cache createServerCache(HazelcastInstance server1) {
        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(server1);
        CacheManager serverCacheManager = provider.getCacheManager();

        return serverCacheManager.createCache(CACHE_NAME, cacheConfig);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void smoke_near_cache_population() {
        int cacheSize = 1000;

        // 1. populate server side cache
        for (int i = 0; i < cacheSize; i++) {
            serverCache1.put(i, i);
        }

        // 2. add client with Near Cache
        clientCache = createCacheFromNewClient();

        // 3. populate client Near Cache
        for (int i = 0; i < cacheSize; i++) {
            assertNotNull(clientCache.get(i));
        }

        // 4. assert number of entries in client Near Cache
        assertEquals(cacheSize, ((NearCachedClientCacheProxy) clientCache).getNearCache().size());
    }

    private Cache createCacheFromNewClient() {
        HazelcastClientProxy client = (HazelcastClientProxy) factory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        return cacheManager.createCache(CACHE_NAME, cacheConfig);
    }
}
