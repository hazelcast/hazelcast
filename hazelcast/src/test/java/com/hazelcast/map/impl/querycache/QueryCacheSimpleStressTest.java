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
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class QueryCacheSimpleStressTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    private final String mapName = randomString();
    private final String cacheName = randomString();
    private final Config config = new Config();
    private final int numberOfElementsToPut = 10000;

    @Before
    public void setUp() {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        evictionConfig.setSize(Integer.MAX_VALUE);
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig
                .setBufferSize(30)
                .setDelaySeconds(2)
                .setBatchSize(2)
                .setPopulate(true)
                .getPredicateConfig().setImplementation(Predicates.alwaysTrue());
        queryCacheConfig.setEvictionConfig(evictionConfig);

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        config.addMapConfig(mapConfig);
    }

    @Test
    public void testStress() throws Exception {
        final IMap<Integer, Integer> map = getMap();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < numberOfElementsToPut; i++) {
                    map.put(i, i);
                }
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();

        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                queryCache.tryRecover();
            }
        }, true);

        thread.join();

        assertQueryCacheSizeEventually(numberOfElementsToPut, queryCache);
    }

    private <K, V> IMap<K, V> getMap() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance node = instances[0];
        return AbstractQueryCacheTestSupport.getMap(node, mapName);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache queryCache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, queryCache.size());
            }
        };

        assertTrueEventually(task, 20);
    }
}
