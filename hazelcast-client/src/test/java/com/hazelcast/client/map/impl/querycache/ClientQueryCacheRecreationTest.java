/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.impl.querycache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheRecreationTest extends HazelcastTestSupport {

    private final String mapName = "mapName";
    private final String queryCacheName = "queryCacheName";
    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private HazelcastInstance server;

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Before
    public void setUp() {
        server = factory.newHazelcastInstance(getConfig());

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);

        ClientConfig config = new ClientConfig();
        config.addQueryCacheConfig(mapName, queryCacheConfig);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setEnabled(true)
                .setMaxBackoffMillis((int) TimeUnit.SECONDS.toMillis(5));

        client = factory.newHazelcastClient(config);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void query_cache_recreates_itself_after_server_restart() {
        IMap<Object, Object> map = client.getMap(mapName);
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        final QueryCache<Object, Object> queryCache = map.getQueryCache(queryCacheName,
                Predicates.alwaysTrue(), true);

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        server.shutdown();
        server = factory.newHazelcastInstance(getConfig());

        for (int i = 100; i < 200; i++) {
            map.put(i, i);
        }

        InternalQueryCache internalQueryCache = (InternalQueryCache) queryCache;
        internalQueryCache.recreate();

        for (int i = 200; i < 300; i++) {
            map.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(200, queryCache.size());
            }
        });

        Set<Object> keySet = queryCache.keySet();
        for (int i = 0; i < 300; i++) {
            if (i < 100) {
                assertFalse(keySet.contains(i));
            } else {
                assertTrue(keySet.contains(i));
            }
        }
    }

    @Test
    public void listeners_still_works_after_query_cache_recreation() {
        IMap<Object, Object> map = client.getMap(mapName);
        QueryCache<Object, Object> queryCache = map.getQueryCache(queryCacheName,
                Predicates.alwaysTrue(), true);
        final AtomicInteger entryAddedCounter = new AtomicInteger();
        queryCache.addEntryListener(new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                entryAddedCounter.incrementAndGet();
            }
        }, new SqlPredicate("__key >= 10"), true);

        // Restart server
        server.shutdown();
        server = factory.newHazelcastInstance(getConfig());

        // Recreate query cache on same reference
        // Recreation empties query cache.
        ((InternalQueryCache) queryCache).recreate();

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        AssertTask assertTask = new AssertTask() {
            @Override
            public void run() {
                assertEquals(90, entryAddedCounter.get());
            }
        };

        assertTrueEventually(assertTask);
        assertTrueAllTheTime(assertTask, 3);
    }

}
