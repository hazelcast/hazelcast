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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheCreateDestroyTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void create_after_destroy_gives_fresh_query_cache_instance() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        HazelcastInstance server = factory.newHazelcastInstance(newConfigWithQueryCache(mapName, queryCacheName));
        server.getMap(mapName);

        IMap map = server.getMap(mapName);
        QueryCache queryCache = map.getQueryCache(queryCacheName);

        queryCache.destroy();
        QueryCache newQueryCache = map.getQueryCache(queryCacheName);

        assertFalse(queryCache == newQueryCache);
    }

    @Test
    public void recreated_queryCache_gets_updates_from_restarted_server() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        // start a server
        HazelcastInstance server = factory.newHazelcastInstance();
        server.getMap(mapName);

        // start a serverWithQueryCache
        HazelcastInstance serverWithQueryCache = factory.newHazelcastInstance(newConfigWithQueryCache(mapName, queryCacheName));

        // create query-cache
        IMap map = serverWithQueryCache.getMap(mapName);
        QueryCache queryCache = map.getQueryCache(queryCacheName);

        // shutdown other member, at this point only serverWithQueryCache is alive
        server.shutdown();

        // start new server to emulate server re-start
        HazelcastInstance newServer = factory.newHazelcastInstance();
        IMap newServerMap = newServer.getMap(mapName);

        // populate new map from newServer
        for (int i = 0; i < 1000; i++) {
            newServerMap.put(i, i);
        }

        // destroy query-cache from serverWithQueryCache
        queryCache.destroy();

        // new query-cache should have all updates from server side
        queryCache = map.getQueryCache(queryCacheName);
        int queryCacheSize = queryCache.size();

        assertEquals(1000, queryCacheSize);
    }

    @Test
    public void tryRecover_fails_after_destroy() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        HazelcastInstance server = factory.newHazelcastInstance(newConfigWithQueryCache(mapName, queryCacheName));
        server.getMap(mapName);

        IMap map = server.getMap(mapName);
        QueryCache queryCache = map.getQueryCache(queryCacheName);

        queryCache.destroy();

        assertFalse(queryCache.tryRecover());
    }


    private static Config newConfigWithQueryCache(String mapName, String queryCacheName) {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.getPredicateConfig().setImplementation(Predicates.alwaysTrue());
        queryCacheConfig.setPopulate(true);

        Config config = new Config();
        config.getMapConfig(mapName).addQueryCacheConfig(queryCacheConfig);

        return config;
    }
}
