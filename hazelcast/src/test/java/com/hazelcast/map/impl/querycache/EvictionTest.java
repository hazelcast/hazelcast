package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EvictionTest extends HazelcastTestSupport {

    @Test
    public void testMaxSizeEvictionWorks() {
        int maxSize = 100;
        int populationCount = 500;

        String mapName = randomString();
        String cacheName = randomString();

        Config config = getConfig(maxSize, mapName, cacheName);
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = getMap(node, mapName);

        // expecting at least populationCount - maxSize - 50 evicted entries according to max size.
        // 50 states an error margin since eviction does not sweep precise number of entries.
        int margin = 50;
        final CountDownLatch evictedCount = new CountDownLatch(populationCount - maxSize - margin);
        QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        String listener = cache.addEntryListener(new EntryEvictedListener() {
            @Override
            public void entryEvicted(EntryEvent event) {
                evictedCount.countDown();
            }
        }, false);

        for (int i = 0; i < populationCount; i++) {
            map.put(i, i);
        }

        assertOpenEventually("Cache size is " + cache.size(), evictedCount);
        assertQueryCacheEvicted(maxSize, margin, cache);
        cache.removeEntryListener(listener);
    }


    private Config getConfig(int maxSize, String mapName, String cacheName) {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        EvictionConfig evictionConfig = cacheConfig.getEvictionConfig();
        evictionConfig.setSize(maxSize);
        evictionConfig.setEvictionPolicy(EvictionPolicy.LFU);
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);

        mapConfig.addQueryCacheConfig(cacheConfig);
        return config;
    }

    private void assertQueryCacheEvicted(int maxSize, int margin, QueryCache<Integer, Integer> cache) {
        int size = cache.size();
        assertTrue("cache size = " + size + ", should be smaller than max size = " + maxSize, size < maxSize + margin);
    }
}
