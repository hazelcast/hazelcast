package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Random;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheEvictionTest extends HazelcastTestSupport {

    CachingProvider provider;
    HazelcastInstance instance;
    Cache<Integer, Integer> cache;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        provider = HazelcastServerCachingProvider.createCachingProvider(instance);
        CacheManager cacheManager = provider.getCacheManager();
        String cacheName = randomString();
        cache = cacheManager.createCache(cacheName, new CacheConfig());
    }

    @Test
    public void testExpirationTaskShouldNotEvictRecords() {
        int elementCount = 50;
        for (int i = 0; i < elementCount; i++) {
            cache.put(i, 0);
        }

        Random random = new Random(System.currentTimeMillis());
        int count = 1200;
        for (int i = 0; i < count; i++) {
            int key = random.nextInt(elementCount);
            // if some records get evicted we will get NPE here
            int value = cache.get(key);
            int inc = random.nextInt(10);
            cache.put(key, value + inc);
            sleepMillis(10);
            //we sleep (12 sec total) just for ExpirationTask to kick in
        }

    }

}
