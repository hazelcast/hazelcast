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
import com.hazelcast.util.EmptyStatement;
import org.junit.After;
import org.junit.Before;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.impl.maxsize.impl.EntryCountCacheMaxSizeChecker.calculateMaxPartitionSize;
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
        if (cacheManager != null) {
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
        Node node = getNode(getHazelcastInstance());
        if (node != null) {
            return node.getProperties().getInteger(GroupProperty.PARTITION_COUNT);
        }
        return Integer.valueOf(GroupProperty.PARTITION_COUNT.getDefaultValue());
    }

    protected void assertThatNoCacheEvictionHappened(ICache cache) {
        try {
            assertEquals("there should be no evicted values", 0, cache.getLocalCacheStatistics().getCacheEvictions());
        } catch (UnsupportedOperationException e) {
            // cache statistics are not supported on clients yet
            EmptyStatement.ignore(e);
        }
    }
}
