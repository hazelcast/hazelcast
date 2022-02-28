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

package com.hazelcast.client.map.impl.querycache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    private static TestHazelcastFactory factory = new TestHazelcastFactory();

    @BeforeClass
    public static void setUp() throws Exception {
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testQueryCache_whenIncludeValueEnabled() {
        boolean includeValue = true;
        testQueryCache(includeValue);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testQueryCache_whenIncludeValueDisabled() {
        boolean includeValue = false;
        testQueryCache(includeValue);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testQueryCache_whenInitialPopulationEnabled() {
        boolean enableInitialPopulation = true;
        int numberOfElementsToBePutToIMap = 1000;
        int expectedSizeOfQueryCache = numberOfElementsToBePutToIMap;

        testWithInitialPopulation(enableInitialPopulation, expectedSizeOfQueryCache, numberOfElementsToBePutToIMap);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testQueryCache_whenInitialPopulationDisabled() {
        boolean enableInitialPopulation = false;
        int numberOfElementsToBePutToIMap = 1000;
        int expectedSizeOfQueryCache = 0;

        testWithInitialPopulation(enableInitialPopulation, expectedSizeOfQueryCache, numberOfElementsToBePutToIMap);
    }

    @Test
    public void testQueryCache_withLocalListener() {
        String mapName = randomString();
        String queryCacheName = randomString();
        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Integer, Integer> map = client.getMap(mapName);

        for (int i = 0; i < 30; i++) {
            map.put(i, i);
        }
        final AtomicInteger countAddEvent = new AtomicInteger();
        final AtomicInteger countRemoveEvent = new AtomicInteger();

        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(queryCacheName, new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                countAddEvent.incrementAndGet();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                countRemoveEvent.incrementAndGet();
            }
        }, Predicates.sql("this > 20"), true);

        for (int i = 0; i < 30; i++) {
            map.remove(i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, queryCache.size());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Count of add events wrong!", 9, countAddEvent.get());
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Count of remove events wrong!", 9, countRemoveEvent.get());
            }
        });
    }

    @Test
    public void testQueryCacheCleared_afterCalling_IMap_evictAll() {
        String cacheName = randomString();
        final IMap<Integer, Integer> map = getMap();
        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        IFunction evictAll = new IFunction() {
            @Override
            public Object apply(Object ignored) {
                map.evictAll();
                return null;
            }
        };

        assertQueryCacheSizeEventually(0, evictAll, queryCache);
    }

    @Test
    public void testQueryCacheCleared_afterCalling_IMap_clear() {
        String cacheName = randomString();
        final IMap<Integer, Integer> map = getMap();
        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        IFunction clear = new IFunction() {

            @Override
            public Object apply(Object ignored) {
                map.clear();
                return null;
            }
        };

        assertQueryCacheSizeEventually(0, clear, queryCache);
    }

    @Test
    public void testDestroy_emptiesQueryCache() {
        int entryCount = 1000;
        final CountDownLatch numberOfAddEvents = new CountDownLatch(entryCount);
        String cacheName = randomString();
        IMap<Integer, Integer> map = getMap();
        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                numberOfAddEvents.countDown();
            }
        }, TRUE_PREDICATE, false);

        for (int i = 0; i < entryCount; i++) {
            map.put(i, i);
        }

        assertOpenEventually(numberOfAddEvents);

        queryCache.destroy();

        assertEquals(0, queryCache.size());
    }

    private void testWithInitialPopulation(boolean enableInitialPopulation, int expectedSize, int numberOfElementsToPut) {
        String mapName = randomString();
        String cacheName = randomName();

        ClientConfig config = getConfig(cacheName, enableInitialPopulation, mapName);
        HazelcastInstance client = factory.newHazelcastClient(config);

        IMap<Integer, Integer> map = client.getMap(mapName);
        for (int i = 0; i < numberOfElementsToPut; i++) {
            map.put(i, i);
        }
        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);

        assertEquals(expectedSize, queryCache.size());
    }

    private void testQueryCache(boolean includeValue) {
        String mapName = randomString();
        String queryCacheName = randomString();

        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Integer, Integer> map = client.getMap(mapName);

        for (int i = 0; i < 50; i++) {
            map.put(i, i);
        }
        Predicate<Integer, Integer> predicate = Predicates.sql("this > 5 AND this < 100");
        QueryCache<Integer, Integer> cache = map.getQueryCache(queryCacheName, predicate, includeValue);

        for (int i = 50; i < 100; i++) {
            map.put(i, i);
        }

        int expected = 94;
        assertQueryCacheSize(expected, cache);
    }

    private ClientConfig getConfig(String queryCacheName, boolean enableInitialPopulation, String mapName) {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig
                .setPopulate(enableInitialPopulation)
                .getPredicateConfig().setImplementation(Predicates.alwaysTrue());

        return addConfig(queryCacheConfig, mapName);
    }

    private ClientConfig addConfig(QueryCacheConfig queryCacheConfig, String mapName) {
        ClientConfig clientConfig = new ClientConfig();
        Map<String, Map<String, QueryCacheConfig>> queryCacheConfigs = clientConfig.getQueryCacheConfigs();
        Map<String, QueryCacheConfig> queryCacheConfigMap = queryCacheConfigs.get(mapName);
        if (queryCacheConfigMap == null) {
            queryCacheConfigMap = new HashMap<String, QueryCacheConfig>();
            queryCacheConfigs.put(mapName, queryCacheConfigMap);
        }
        queryCacheConfigMap.put(queryCacheConfig.getName(), queryCacheConfig);
        return clientConfig;
    }

    private IMap<Integer, Integer> getMap() {
        String mapName = randomString();
        HazelcastInstance client = factory.newHazelcastClient();
        return client.getMap(mapName);
    }

    private void assertQueryCacheSize(final int expected, final QueryCache cache) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, cache.size());
            }
        }, 20);
    }

    private void assertQueryCacheSizeEventually(final int expected, final IFunction<?, ?> function, final QueryCache queryCache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                if (function != null) {
                    function.apply(null);
                }
                assertEquals(expected, queryCache.size());
            }
        };

        assertTrueEventually(task);
    }
}
