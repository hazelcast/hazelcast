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
import javax.cache.spi.CachingProvider;

/**
 * @author mdogan 02/06/14
 */
public abstract class CacheTestSupport extends HazelcastTestSupport {

    protected CachingProvider cachingProvider;
    protected CacheManager cacheManager;

    protected abstract HazelcastInstance getHazelcastInstance();

    @Before
    public void setup() {
        onSetup();
        cachingProvider = getCachingProvider();
        cacheManager = cachingProvider.getCacheManager();
    }

    @After
    public void tearDown() {
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
        Cache<K, V> cache = cacheManager.createCache(cacheName, createCacheConfig());
        return cache.unwrap(ICache.class);
    }

    protected <K, V> ICache<K, V> createCache(String cacheName) {
        Cache<K, V> cache = cacheManager.createCache(cacheName, createCacheConfig());
        return cache.unwrap(ICache.class);
    }

    protected <K, V> ICache<K, V> createCache(CacheConfig<K, V> config) {
        String cacheName = randomString();
        Cache<K, V> cache = cacheManager.createCache(cacheName, config);
        return (ICache<K, V>) cache;
    }

    protected <K, V> ICache<K, V> createCache(String cacheName, CacheConfig<K, V> config) {
        Cache<K, V> cache = cacheManager.createCache(cacheName, config);
        return (ICache<K, V>) cache;
    }

    public Config createConfig() {
        return new Config();
    }

    public CacheConfig createCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        cacheConfig.setStatisticsEnabled(true);
        return cacheConfig;
    }

    protected CachingProvider getCachingProvider() {
        return HazelcastServerCachingProvider
                .createCachingProvider(getHazelcastInstance());
    }

}
