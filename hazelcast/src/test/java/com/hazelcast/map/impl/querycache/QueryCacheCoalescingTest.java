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
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheCoalescingTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    @Test
    public void testCoalescingModeWorks() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = getConfig(mapName, cacheName);
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = getMap(node, mapName);

        final CountDownLatch updateEventCount = new CountDownLatch(1);
        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addEntryListener(new EntryUpdatedListener() {
            @Override
            public void entryUpdated(EntryEvent event) {
                updateEventCount.countDown();
            }
        }, false);

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        // update same key to control whether coalescing kicks in.
        for (int i = 0; i < 500; i++) {
            map.put(0, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(100, cache.size());
            }
        });
        assertOpenEventually(updateEventCount);
    }

    private Config getConfig(String mapName, String cacheName) {
        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName)
                .setCoalesce(true)
                .setBatchSize(64)
                .setBufferSize(64)
                .setDelaySeconds(3);

        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1");

        config.getMapConfig(mapName)
                .addQueryCacheConfig(cacheConfig);

        return config;
    }
}
