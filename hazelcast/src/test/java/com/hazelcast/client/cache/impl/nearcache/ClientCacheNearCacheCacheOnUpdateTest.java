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
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.nearcache.NearCacheStats;
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
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheCacheOnUpdateTest extends ClientNearCacheTestSupport {

    private static final int NUM_OF_KEYS = 100;

    @Test
    public void with_cacheOnUpdate_policy_concurrently_updated_near_cache_does_not_cause_any_miss() {
        ICache<Integer, Integer> nearCachedClientCache = newNearCachedCache(CACHE_ON_UPDATE);
        checkNearCacheInstance(nearCachedClientCache);

        runTest(nearCachedClientCache, CACHE_ON_UPDATE);

        NearCache nearCache = ((NearCachedClientCacheProxy) nearCachedClientCache).getNearCache();
        int size = nearCache.size();
        NearCacheStats nearCacheStatistics = nearCachedClientCache.getLocalCacheStatistics()
                .getNearCacheStatistics();
        assertEquals(size + ", " + nearCacheStatistics.toString()
                + ", " + nearCache.getNearCacheConfig(), 0, nearCacheStatistics.getMisses());
    }

    protected void checkNearCacheInstance(ICache iCacheOnClient) {
        NearCache nearCache = ((NearCachedClientCacheProxy<Object, Object>) iCacheOnClient).getNearCache();
        assertInstanceOf(DefaultNearCache.class, nearCache);
    }

    @Test
    public void with_invalidate_policy_concurrently_updated_near_cache_causes_misses() {
        ICache<Integer, Integer> nearCachedClientCache = newNearCachedCache(INVALIDATE);
        checkNearCacheInstance(nearCachedClientCache);

        runTest(nearCachedClientCache, INVALIDATE);

        NearCacheStats nearCacheStatistics = nearCachedClientCache.getLocalCacheStatistics()
                .getNearCacheStatistics();
        assertTrue(nearCacheStatistics.toString(), nearCacheStatistics.getMisses() > 0);
    }

    private static void runTest(ICache<Integer, Integer> icacheOnClient,
                                NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        for (int i = 0; i < NUM_OF_KEYS; i++) {
            icacheOnClient.put(i, i);

            if (localUpdatePolicy.equals(INVALIDATE)) {
                // populate near-cache here for invalidate policy.
                icacheOnClient.get(i);
            }
        }

        assertEquals(NUM_OF_KEYS, ((NearCachedClientCacheProxy) icacheOnClient).getNearCache().size());

        AtomicBoolean stop = new AtomicBoolean();

        Runnable getter = () -> {
            int i = 0;
            while (!stop.get()) {
                icacheOnClient.get(i++ % NUM_OF_KEYS);
            }
        };

        Runnable putter = () -> {
            int i = 0;
            while (!stop.get()) {
                i = i++ % NUM_OF_KEYS;
                icacheOnClient.put(i, i);
            }
        };

        ExecutorService executor = Executors.newCachedThreadPool();

        int numOfGetters = 2 * RuntimeAvailableProcessors.get();
        for (int i = 0; i < numOfGetters; i++) {
            executor.execute(getter);
        }

        executor.execute(putter);

        sleepSeconds(10);
        stop.set(true);

        executor.shutdown();
        try {
            executor.awaitTermination(100, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("InterruptedException");
        } finally {
            executor.shutdownNow();
        }
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
                .setInMemoryFormat(nearCacheInMemoryFormat())
                .setLocalUpdatePolicy(localUpdatePolicy);
    }

    protected InMemoryFormat nearCacheInMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    private static <K, V> CacheConfig<K, V> newCacheConfig(InMemoryFormat inMemoryFormat) {
        return new CacheConfig<K, V>()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(1);
    }

}
