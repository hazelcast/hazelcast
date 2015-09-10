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

    protected ICache createCache() {
        String cacheName = randomString();
        Cache<Object, Object> cache = cacheManager.createCache(cacheName, createCacheConfig());
        return cache.unwrap(ICache.class);
    }

    protected ICache createCache(String cacheName) {
        Cache<Object, Object> cache = cacheManager.createCache(cacheName, createCacheConfig());
        return cache.unwrap(ICache.class);
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
