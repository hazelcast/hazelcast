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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheTest extends AbstractQueryCacheTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> TRUE_PREDICATE = TruePredicate.INSTANCE;
    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> SQL_PREDICATE = new SqlPredicate("this > 20");

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testQueryCache_whenIncludeValue_enabled() {
        boolean includeValue = true;
        testQueryCache(includeValue);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testQueryCache_whenIncludeValue_disabled() {
        boolean includeValue = false;
        testQueryCache(includeValue);
    }

    @Test
    @SuppressWarnings({"ConstantConditions", "UnnecessaryLocalVariable"})
    public void testQueryCache_whenInitialPopulation_enabled() {
        boolean enableInitialPopulation = true;
        int numberOfElementsToBePutToIMap = 1000;
        int expectedSizeOfQueryCache = numberOfElementsToBePutToIMap;

        testWithInitialPopulation(enableInitialPopulation, expectedSizeOfQueryCache, numberOfElementsToBePutToIMap);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testQueryCache_whenInitialPopulation_disabled() throws Exception {
        boolean enableInitialPopulation = false;
        int numberOfElementsToBePutToIMap = 1000;
        int expectedSizeOfQueryCache = 0;

        testWithInitialPopulation(enableInitialPopulation, expectedSizeOfQueryCache, numberOfElementsToBePutToIMap);
    }

    @Test
    public void testQueryCache_withLocalListener() {
        Config config = new Config().setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        config.getMapConfig(mapName).addQueryCacheConfig(queryCacheConfig);

        IMap<Integer, Integer> map = getIMap(config);

        for (int i = 0; i < 30; i++) {
            map.put(i, i);
        }
        final AtomicInteger countAddEvent = new AtomicInteger();
        final AtomicInteger countRemoveEvent = new AtomicInteger();

        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                countAddEvent.incrementAndGet();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                countRemoveEvent.incrementAndGet();
            }
        }, SQL_PREDICATE, true);

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
        final IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);
        QueryCache<Integer, Employee> queryCache = map.getQueryCache(cacheName);

        populateMap(map, 1000);

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
        final IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);
        final QueryCache<Integer, Employee> queryCache = map.getQueryCache(cacheName);

        populateMap(map, 1000);

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
    public void testGetName() {
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);
        QueryCache<Integer, Employee> queryCache = map.getQueryCache(cacheName);

        assertEquals(cacheName, queryCache.getName());
    }

    @Test
    public void testDestroy_emptiesQueryCache() {
        int entryCount = 1000;
        final CountDownLatch numberOfAddEvents = new CountDownLatch(entryCount);
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);
        QueryCache<Integer, Employee> queryCache
                = map.getQueryCache(cacheName, new EntryAddedListener<Integer, Employee>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Employee> event) {
                numberOfAddEvents.countDown();
            }
        }, TRUE_PREDICATE, false);

        populateMap(map, entryCount);

        assertOpenEventually(numberOfAddEvents);

        queryCache.destroy();

        assertEquals(0, queryCache.size());
    }

    @Test(expected = NullPointerException.class)
    public void getAll_throws_exception_when_supplied_keySet_contains_null_key() {
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);
        QueryCache<Integer, Employee> queryCache = map.getQueryCache(cacheName);

        Set<Integer> keySet = new HashSet<Integer>();
        keySet.add(1);
        keySet.add(2);
        keySet.add(null);

        queryCache.getAll(keySet);
    }

    @Test
    public void getAll_with_non_existent_keys() {
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);
        QueryCache<Integer, Employee> queryCache = map.getQueryCache(cacheName);

        Set<Integer> keySet = new HashSet<Integer>();
        keySet.add(1);
        keySet.add(2);

        queryCache.getAll(keySet);
    }

    @Test
    public void testQueryCache_with_mapAttribute_inPredicate() {

        String MAP_ATTRIBUTE_NAME = "booleanMapAttribute";

        Config config = new Config();
        config.getMapConfig(mapName)
                .addQueryCacheConfig(
                        new QueryCacheConfig(cacheName)
                                .setIncludeValue(true)
                                .setPredicateConfig(// use map attribute in a predicate
                                        new PredicateConfig(Predicates.equal(MAP_ATTRIBUTE_NAME, true))
                                ))
                .addMapAttributeConfig(
                        new MapAttributeConfig()
                                .setExtractor(EvenNumberEmployeeValueExtractor.class.getName())
                                .setName(MAP_ATTRIBUTE_NAME));

        IMap<Integer, Employee> map = getIMap(config);
        QueryCache<Integer, Employee> queryCache = map.getQueryCache(cacheName);

        populateMap(map, 100);

        assertQueryCacheSizeEventually(50, queryCache);
    }

    public static class EvenNumberEmployeeValueExtractor extends ValueExtractor<Employee, Integer> {
        @Override
        public void extract(Employee target, Integer argument, ValueCollector collector) {
            collector.addObject(target.getId() % 2 == 0);
        }
    }

    @SuppressWarnings("unchecked")
    private void testQueryCache(boolean includeValue) {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig.setIncludeValue(includeValue);

        Config config = new Config();
        config.getMapConfig(mapName).addQueryCacheConfig(queryCacheConfig);

        IMap<Integer, Integer> map = getIMap(config);

        String cacheName = randomString();

        for (int i = 0; i < 50; i++) {
            map.put(i, i);
        }
        Predicate<Integer, Integer> predicate = new SqlPredicate("this > 5 AND this < 100");
        QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, predicate, includeValue);

        for (int i = 50; i < 100; i++) {
            map.put(i, i);
        }

        int expected = 94;
        assertQueryCacheSizeEventually(expected, cache);
    }

    private void testWithInitialPopulation(boolean enableInitialPopulation,
                                           int expectedSize, int numberOfElementsToPut) {

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig.setPopulate(enableInitialPopulation);

        Config config = new Config();
        config.getMapConfig(mapName).addQueryCacheConfig(queryCacheConfig);

        IMap<Integer, Employee> map = getIMap(config);

        populateMap(map, numberOfElementsToPut);
        QueryCache<Integer, Employee> queryCache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);

        assertEquals(expectedSize, queryCache.size());
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertQueryCacheSizeEventually(final int expected, final IFunction<?, ?> function,
                                                       final QueryCache queryCache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                function.apply(null);
                assertEquals(expected, queryCache.size());
            }
        };

        assertTrueEventually(task);
    }

    private static void assertQueryCacheSizeEventually(final int expected, final QueryCache cache) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, cache.size());
            }
        });
    }
}
