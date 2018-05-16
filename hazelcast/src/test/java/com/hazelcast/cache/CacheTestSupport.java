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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.impl.maxsize.impl.EntryCountCacheEvictionChecker.calculateMaxPartitionSize;
import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;

public abstract class CacheTestSupport extends HazelcastTestSupport {

    protected CachingProvider cachingProvider;
    protected CacheManager cacheManager;

    protected abstract HazelcastInstance getHazelcastInstance();

    @Before
    public final void setup() {
        onSetup();
        cachingProvider = getCachingProvider();
        cacheManager = cachingProvider.getCacheManager();
    }

    @After
    public final void tearDown() {
        if (cacheManager != null && !cacheManager.isClosed()) {
            Iterable<String> cacheNames = cacheManager.getCacheNames();
            for (String name : cacheNames) {
                cacheManager.destroyCache(name);
            }
            cacheManager.close();
        }
        if (cachingProvider != null) {
            cachingProvider.close();
        }
        onTearDown();
    }

    protected abstract void onSetup();

    protected abstract void onTearDown();

    protected <K, V> ICache<K, V> createCache() {
        String cacheName = randomString();
        Cache<K, V> cache = cacheManager.<K, V, Configuration>createCache(cacheName, createCacheConfig());
        return (ICache<K, V>) cache;
    }

    protected <K, V> ICache<K, V> createCache(String cacheName) {
        Cache<K, V> cache = cacheManager.<K, V, Configuration>createCache(cacheName, createCacheConfig());
        return (ICache<K, V>) cache;
    }

    protected <K, V> ICache<K, V> createCache(CacheConfig<K, V> config) {
        String cacheName = randomString();
        Cache<K, V> cache = cacheManager.<K, V, Configuration>createCache(cacheName, config);
        return (ICache<K, V>) cache;
    }

    protected <K, V> ICache<K, V> createCache(String cacheName, CacheConfig<K, V> config) {
        Cache<K, V> cache = cacheManager.<K, V, Configuration>createCache(cacheName, config);
        return (ICache<K, V>) cache;
    }

    protected Config createConfig() {
        return new Config();
    }

    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>();
        cacheConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        cacheConfig.setStatisticsEnabled(true);
        return cacheConfig;
    }

    protected <K, V> CacheConfig<K, V> getCacheConfigWithMaxSize(int maxCacheSize) {
        CacheConfig<K, V> config = createCacheConfig();
        config.getEvictionConfig().setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        config.getEvictionConfig().setSize(maxCacheSize);
        return config;
    }

    protected CachingProvider getCachingProvider() {
        return getCachingProvider(getHazelcastInstance());
    }

    protected CachingProvider getCachingProvider(HazelcastInstance instance) {
        return HazelcastServerCachingProvider.createCachingProvider(instance);
    }

    protected int getMaxCacheSizeWithoutEviction(CacheConfig cacheConfig) {
        int maxEntryCount = cacheConfig.getEvictionConfig().getSize();
        return calculateMaxPartitionSize(maxEntryCount, getPartitionCount());
    }

    protected int getMaxCacheSizeWithEviction(CacheConfig cacheConfig) {
        int maxEntryCount = cacheConfig.getEvictionConfig().getSize();
        int partitionCount = getPartitionCount();
        return 10 * partitionCount * calculateMaxPartitionSize(maxEntryCount, partitionCount);
    }

    private int getPartitionCount() {
        try {
            Node node = getNode(getHazelcastInstance());
            return node.getProperties().getInteger(GroupProperty.PARTITION_COUNT);
        } catch (IllegalArgumentException e) {
            return parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        }
    }

    protected void assertThatNoCacheEvictionHappened(ICache cache) {
        try {
            assertEquals("there should be no evicted values", 0, cache.getLocalCacheStatistics().getCacheEvictions());
        } catch (UnsupportedOperationException e) {
            // cache statistics are not supported on clients yet
            ignore(e);
        }
    }
}
