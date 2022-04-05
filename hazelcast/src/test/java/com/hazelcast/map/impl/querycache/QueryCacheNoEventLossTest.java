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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.RandomPicker.getInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheNoEventLossTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "mapName";
    private static final String QUERY_CACHE_NAME = "cacheName";
    private static final int TEST_DURATION_SECONDS = 3;

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Test
    public void no_event_lost_during_migrations__with_one_parallel_node() {
        no_event_lost_during_migrations(1, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void no_event_lost_during_migrations__with_many_parallel_nodes() {
        assertTrueEventually(() -> {
            try {
                no_event_lost_during_migrations(3, 5);
            } finally {
                factory.shutdownAll();
            }
        });
    }

    private void no_event_lost_during_migrations(int parallelNodeCount, int startNewNodeAfterSeconds) {
        final AtomicInteger eventLostCounter = new AtomicInteger();
        newQueryCacheOnNewNode(eventLostCounter);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < parallelNodeCount; i++) {
            // creates CLEAR_ALL events
            Thread doMapClear = new Thread(() -> {
                IMap map = newQueryCacheOnNewNode(eventLostCounter);

                long endMillis = System.currentTimeMillis() + SECONDS.toMillis(TEST_DURATION_SECONDS);
                while (System.currentTimeMillis() < endMillis) {

                    int key = getInt(10);
                    int value = getInt(1000);
                    map.put(key, value);

                    map.clear();
                }
            });

            threads.add(doMapClear);
        }

        for (Thread thread : threads) {
            thread.start();
            sleepSeconds(startNewNodeAfterSeconds);
        }

        assertJoinable(threads.toArray(new Thread[0]));

        assertTrueEventually(() -> assertAllQueryCachesSyncWithMap());

        assertEquals(0, eventLostCounter.get());
    }

    private void assertAllQueryCachesSyncWithMap() {
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();

        for (HazelcastInstance instance : instances) {
            IMap map = instance.getMap(MAP_NAME);
            Set<Map.Entry> mapEntrySet = map.entrySet();

            QueryCache queryCache = map.getQueryCache(QUERY_CACHE_NAME);
            Set<Map.Entry> queryCacheEntrySet = queryCache.entrySet();

            assertEquals(queryCacheEntrySet.size(), mapEntrySet.size());

            for (Map.Entry entry : mapEntrySet) {
                Object key = entry.getKey();

                Object valueFromMap = entry.getValue();
                Object valueFromQueryCache = queryCache.get(key);

                assertEquals(valueFromQueryCache, valueFromMap);
            }
        }
    }

    private IMap newQueryCacheOnNewNode(final AtomicInteger eventLostCounter) {
        HazelcastInstance node = factory.newHazelcastInstance(newConfig());
        waitClusterForSafeState(node);
        IMap map = node.getMap(MAP_NAME);
        addEventLostListenerToQueryCache(map, eventLostCounter);
        return map;
    }

    private void addEventLostListenerToQueryCache(IMap map, final AtomicInteger eventLostCounter) {
        map.getQueryCache(QUERY_CACHE_NAME)
                .addEntryListener((EventLostListener) event -> eventLostCounter.incrementAndGet(), false);
    }

    private Config newConfig() {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig()
                .setName(QUERY_CACHE_NAME)
                .setPredicateConfig(new PredicateConfig(Predicates.alwaysTrue()));

        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .addQueryCacheConfig(queryCacheConfig);

        return smallInstanceConfig()
                .addMapConfig(mapConfig);
    }
}
