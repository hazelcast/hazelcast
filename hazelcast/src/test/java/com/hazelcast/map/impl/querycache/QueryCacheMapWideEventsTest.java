/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheMapWideEventsTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "mapName";
    private static final String QUERY_CACHE_NAME = "cacheName";
    private static final String PARTITION_COUNT = "1999";
    private static final int TEST_DURATION_SECONDS = 3;

    private AtomicInteger eventLostCounter = new AtomicInteger();
    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Test
    public void no_event_lost_during_migrations() {
        newQueryCacheOnNewNode();

        // creates CLEAR_ALL events
        Thread clearAllNode = new Thread() {
            @Override
            public void run() {
                IMap map = newQueryCacheOnNewNode();

                long endMillis = System.currentTimeMillis() + SECONDS.toMillis(TEST_DURATION_SECONDS);
                while (System.currentTimeMillis() < endMillis) {
                    map.clear();
                }
            }
        };

        clearAllNode.start();
        assertJoinable(clearAllNode);

        assertEquals(0, eventLostCounter.get());
    }

    private IMap newQueryCacheOnNewNode() {
        HazelcastInstance node = factory.newHazelcastInstance(newConfig());
        IMap map = node.getMap(MAP_NAME);
        QueryCache queryCache = map.getQueryCache(QUERY_CACHE_NAME);
        addEventLostListenerToQueryCache(queryCache);
        return map;
    }

    private void addEventLostListenerToQueryCache(QueryCache queryCache) {
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                eventLostCounter.incrementAndGet();
            }
        }, false);
    }

    private Config newConfig() {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig()
                .setName(QUERY_CACHE_NAME)
                .setPredicateConfig(new PredicateConfig(TruePredicate.INSTANCE));

        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .addQueryCacheConfig(queryCacheConfig);

        return getConfig()
                .setProperty("hazelcast.partition.count", PARTITION_COUNT)
                .addMapConfig(mapConfig);
    }
}
