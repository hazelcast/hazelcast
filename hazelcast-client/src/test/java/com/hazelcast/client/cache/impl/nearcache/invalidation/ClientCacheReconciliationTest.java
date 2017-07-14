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

package com.hazelcast.client.cache.impl.nearcache.invalidation;

import com.hazelcast.cache.ICache;
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
import com.hazelcast.monitor.NearCacheStats;
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
import static com.hazelcast.map.impl.nearcache.invalidation.MemberMapReconciliationTest.assertStats;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.valueOf;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheReconciliationTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "test";
    private static final int RECONCILIATION_INTERVAL_SECONDS = 3;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final ClientConfig clientConfig = new ClientConfig();
    private final CacheConfig cacheConfig = new CacheConfig();

    private Cache serverCache;
    private Cache clientCache;

    @Before
    public void setUp() throws Exception {
        NearCacheConfig nearCacheConfig = new NearCacheConfig(CACHE_NAME);
        nearCacheConfig.setInvalidateOnChange(true);

        clientConfig.setProperty("hazelcast.invalidation.max.tolerated.miss.count", "0");
        clientConfig.setProperty("hazelcast.invalidation.reconciliation.interval.seconds", valueOf(RECONCILIATION_INTERVAL_SECONDS));
        clientConfig.setProperty("hazelcast.invalidation.min.reconciliation.interval.seconds", valueOf(RECONCILIATION_INTERVAL_SECONDS));
        clientConfig.addNearCacheConfig(nearCacheConfig);

        cacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        Config config = new Config();
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(Integer.MAX_VALUE));
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(Integer.MAX_VALUE));

        HazelcastInstance server = factory.newHazelcastInstance(config);

        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(server);
        CacheManager serverCacheManager = provider.getCacheManager();

        serverCache = serverCacheManager.createCache(CACHE_NAME, cacheConfig);

        clientCache = createCacheFromNewClient();
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    private Cache createCacheFromNewClient() {
        HazelcastClientProxy client = (HazelcastClientProxy) factory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        Cache cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        assert cache instanceof NearCachedClientCacheProxy;

        return cache;
    }

    @Test
    public void test_reconciliation_does_not_cause_premature_removal() throws Exception {
        int total = 100;
        for (int i = 0; i < total; i++) {
            serverCache.put(i, i);
        }

        for (int i = 0; i < total; i++) {
            clientCache.get(i);
        }

        Cache cacheFromNewClient = createCacheFromNewClient();
        for (int i = 0; i < total; i++) {
            cacheFromNewClient.get(i);
        }

        NearCacheStats nearCacheStats = ((ICache) cacheFromNewClient).getLocalCacheStatistics().getNearCacheStatistics();
        assertStats(nearCacheStats, total, 0, total);

        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECONDS);

        for (int i = 0; i < total; i++) {
            cacheFromNewClient.get(i);
        }

        assertStats(nearCacheStats, total, total, total);
    }
}
