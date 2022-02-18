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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

        final CountDownLatch entryCountingLatch = new CountDownLatch(populationCount);
        QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, Predicates.alwaysTrue(), true);
        UUID listener = cache.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                entryCountingLatch.countDown();
            }
        }, false);

        for (int i = 0; i < populationCount; i++) {
            map.put(i, i);
        }

        assertOpenEventually("Cache size is " + cache.size(), entryCountingLatch);

        // expecting at most populationCount - maxSize - 5 entries
        // 5 states an error margin since eviction does not sweep precise number of entries.
        assertQueryCacheEvicted(maxSize, 5, cache);
        assertTrue(cache.removeEntryListener(listener));
    }

    private Config getConfig(int maxSize, String mapName, String cacheName) {
        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        cacheConfig.getEvictionConfig()
                .setSize(maxSize)
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);

        Config config = new Config();

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(cacheConfig);

        return config;
    }

    private void assertQueryCacheEvicted(int maxSize, int margin, QueryCache<Integer, Integer> cache) {
        int size = cache.size();
        assertTrue("cache size = " + size + ", should be smaller than max size = " + maxSize, size < maxSize + margin);
    }
}
