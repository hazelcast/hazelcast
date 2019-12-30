/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheCacheOnUpdateTest extends ClientNearCacheTestSupport {

    private static final int NUM_OF_KEYS = 100;

    @Test
    public void with_cacheOnUpdate_policy_concurrently_updated_near_cache_does_not_cause_any_miss()
            throws InterruptedException {
        ICache<Integer, Integer> nearCachedClientCache = newNearCachedCache(CACHE_ON_UPDATE);

        runTest(nearCachedClientCache, CACHE_ON_UPDATE);

        assertEquals(0, nearCachedClientCache.getLocalCacheStatistics()
                .getNearCacheStatistics().getMisses());
    }

    @Test
    public void with_invalidate_policy_concurrently_updated_near_cache_causes_misses()
            throws InterruptedException {
        ICache<Integer, Integer> nearCachedClientCache = newNearCachedCache(INVALIDATE);

        runTest(nearCachedClientCache, INVALIDATE);

        assertTrue(nearCachedClientCache.getLocalCacheStatistics()
                .getNearCacheStatistics().getMisses() > NUM_OF_KEYS);
    }

    private static void runTest(ICache<Integer, Integer> icacheOnClient,
                                NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) throws InterruptedException {
        for (int i = 0; i < NUM_OF_KEYS; i++) {
            icacheOnClient.put(i, i);

            if (localUpdatePolicy.equals(INVALIDATE)) {
                // populate near-cache here for invalidate policy.
                icacheOnClient.get(i);
            }
        }

        AtomicBoolean stop = new AtomicBoolean();

        Runnable getter = () -> {
            while (!stop.get()) {
                for (int i = 0; i < NUM_OF_KEYS; i++) {
                    icacheOnClient.get(i);
                }
            }
        };

        Runnable putter = () -> {
            while (!stop.get()) {
                for (int i = 0; i < NUM_OF_KEYS; i++) {
                    icacheOnClient.put(i, i);
                }
            }
        };

        ExecutorService executor = Executors.newCachedThreadPool();

        int numOfGetters = 3 * RuntimeAvailableProcessors.get();
        for (int i = 0; i < numOfGetters; i++) {
            executor.execute(getter);
        }

        executor.execute(putter);

        sleepSeconds(10);
        stop.set(true);

        executor.shutdown();
        executor.awaitTermination(120, TimeUnit.SECONDS);
    }

    private ICache<Integer, Integer> newNearCachedCache(NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(localUpdatePolicy);
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider provider = new HazelcastClientCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();

        return cacheManager.createCache(DEFAULT_CACHE_NAME,
                newCacheConfig(InMemoryFormat.BINARY));
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    protected NearCacheConfig getNearCacheConfig(NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        return new NearCacheConfig()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setLocalUpdatePolicy(localUpdatePolicy);
    }

    private static <K, V> CacheConfig<K, V> newCacheConfig(InMemoryFormat inMemoryFormat) {
        return new CacheConfig<K, V>()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(1);
    }

}
