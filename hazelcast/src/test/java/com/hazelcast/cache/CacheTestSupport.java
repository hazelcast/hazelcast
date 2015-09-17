package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

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

    protected Config createConfig() {
        return new Config();
    }

    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>();
        cacheConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        cacheConfig.setStatisticsEnabled(true);
        return cacheConfig;
    }

    protected CachingProvider getCachingProvider() {
        return HazelcastServerCachingProvider.createCachingProvider(getHazelcastInstance());
    }
}
