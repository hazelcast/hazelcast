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
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.utils.Employee;
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

import java.util.Collection;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheDataSyncWithMapTest extends HazelcastTestSupport {

    protected String mapName = randomString();
    protected String cacheName = randomString();
    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    protected IMap<Integer, Employee> map;
    protected QueryCache<Integer, Employee> queryCache;

    @Before
    public void setUp() throws Exception {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig.getPredicateConfig().setImplementation(Predicates.alwaysTrue());

        MapConfig mapConfig = new MapConfig(mapName).addQueryCacheConfig(queryCacheConfig);

        Config config = getConfig();
        config.addMapConfig(mapConfig);

        HazelcastInstance instance = factory.newHazelcastInstance(config);
        map = instance.getMap(mapName);
        queryCache = map.getQueryCache(cacheName);
    }

    @Test
    public void map_and_queryCache_data_sync_after_map_clear() {
        test_map_wide_event(MethodName.CLEAR);
    }

    @Test
    public void map_and_queryCache_data_sync_after_map_evictAll() {
        test_map_wide_event(MethodName.EVICT_ALL);
    }

    private void test_map_wide_event(MethodName methodName) {
        for (int i = 0; i < 3; i++) {

            callMethod(methodName);

            int start = i * 10;
            for (int j = start; j < start + 10; j++) {
                map.put(j, new Employee(j));
            }
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int mapSize = map.size();
                int queryCacheSize = queryCache.size();

                String msg = "query cache should have same size with underlying imap but"
                        + " its size found %d while map size is %d";

                assertEquals(format(msg, queryCacheSize, mapSize), mapSize, queryCacheSize);

                Collection<Employee> valuesInMap = map.values();
                Collection<Employee> valuesInQueryCache = queryCache.values();

                String msg2 = "valuesInMap %s but valuesInQueryCache %s";
                assertTrue(format(msg2, valuesInMap, valuesInQueryCache),
                        valuesInMap.containsAll(valuesInQueryCache));
            }
        });
    }

    private void callMethod(MethodName methodName) {
        switch (methodName) {
            case CLEAR:
                map.clear();
                break;
            case EVICT_ALL:
                map.evictAll();
                break;
        }
    }

    enum MethodName {
        CLEAR,
        EVICT_ALL
    }
}
