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
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ServerQueryCacheRecreationTest extends HazelcastTestSupport {

    private final String mapName = "mapName";
    private final String queryCacheName = "queryCacheName";
    private final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    private HazelcastInstance server;
    private HazelcastInstance serverWithQueryCache;

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Before
    public void setUp() {
        server = factory.newHazelcastInstance(getConfig());

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        Config config = getConfig();
        config.getMapConfig("default").addQueryCacheConfig(queryCacheConfig);
        serverWithQueryCache = factory.newHazelcastInstance(config);
    }

    @Test
    public void query_cache_recreates_itself_after_server_restart() {
        IMap<Object, Object> map = server.getMap(mapName);

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        final QueryCache<Object, Object> queryCache = serverWithQueryCache.getMap(mapName)
                .getQueryCache(queryCacheName,
                        Predicates.alwaysTrue(), true);

        server.shutdown();
        server = factory.newHazelcastInstance(getConfig());

        map = server.getMap(mapName);

        for (int i = 100; i < 200; i++) {
            map.put(i, i);
        }

        waitAllForSafeState(server, serverWithQueryCache);

        // Make sure that all events are processed before calling queryCache#recreate
        assertCacheSizeEventuallyWithTryRecover(queryCache, 200);


        InternalQueryCache internalQueryCache = (InternalQueryCache) queryCache;
        internalQueryCache.recreate();

        for (int i = 200; i < 300; i++) {
            map.put(i, i);
        }

        assertCacheSizeEventuallyWithTryRecover(queryCache, 300);

        Set<Object> keySet = queryCache.keySet();
        for (int i = 0; i < 300; i++) {
            assertTrue(keySet.contains(i));
        }
    }

    @Test
    public void listeners_still_works_after_query_cache_recreation() {
        IMap<Object, Object> map = serverWithQueryCache.getMap(mapName);
        QueryCache<Object, Object> queryCache = map.getQueryCache(queryCacheName,
                Predicates.alwaysTrue(), true);
        final AtomicInteger entryAddedCounter = new AtomicInteger();
        queryCache.addEntryListener(new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                entryAddedCounter.incrementAndGet();
            }
        }, Predicates.sql("__key >= 10"), true);

        // Restart server
        server.shutdown();
        server = factory.newHazelcastInstance(getConfig());

        // Recreate query cache on same reference
        // Recreation empties query cache.
        ((InternalQueryCache) queryCache).recreate();

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        AssertTask assertTask = () -> assertEquals(90, entryAddedCounter.get());

        assertTrueEventually(assertTask);
        assertTrueAllTheTime(assertTask, 3);
    }

    private static void assertCacheSizeEventuallyWithTryRecover(QueryCache cache, int expectedCacheSize) {
        assertTrueEventually(() -> {
            try {
                assertEquals(expectedCacheSize, cache.size());
            } catch (AssertionError e) {
                cache.tryRecover();
                throw e;
            }
        });
    }
}
