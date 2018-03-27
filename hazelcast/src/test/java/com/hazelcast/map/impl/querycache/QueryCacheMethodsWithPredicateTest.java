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

import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheMethodsWithPredicateTest extends AbstractQueryCacheTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> TRUE_PREDICATE = TruePredicate.INSTANCE;

    @Test
    public void testKeySet_onIndexedField() {
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);
        int count = 111;
        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex("id", true);

        populateMap(map, count, 2 * count);

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void testKeySet_onIndexedField_whenIncludeValueFalse() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex("__key", true);

        populateMap(map, count, 2 * count);

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("__key >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void testKeySet_onIndexedField_afterRemovalOfSomeIndexes() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex("id", true);

        populateMap(map, 17, count);

        // just choose arbitrary numbers to prove whether #keySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("id < " + smallerThan), cache);
    }

    @Test
    public void testEntrySet() {
        int count = 1;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex("id", true);

        populateMap(map, count, 2 * count);

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 0;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void testEntrySet_whenIncludeValueFalse() throws Exception {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        cache.addIndex("id", true);

        removeEntriesFromMap(map, 17, count);

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 0;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("id < " + smallerThan), cache);
    }

    @Test
    public void testEntrySet_withIndexedKeys_whenIncludeValueFalse() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        // here adding key index. (key --> integer; value --> Employee)
        cache.addIndex("__key", true);

        removeEntriesFromMap(map, 17, count);

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);
    }

    @Test
    public void testValues() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex("id", true);

        populateMap(map, count, 2 * count);

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void testValues_withoutIndex() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);

        removeEntriesFromMap(map, 17, count);

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);
    }

    @Test
    public void testValues_withoutIndex_whenIncludeValueFalse() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);

        removeEntriesFromMap(map, 17, count);

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 0;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);
    }

    private void assertKeySetSizeEventually(final int expectedSize, final Predicate predicate,
                                            final QueryCache<Integer, Employee> cache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                int size = cache.size();
                Set<Integer> keySet = cache.keySet(predicate);
                assertEquals("cache size = " + size, expectedSize, keySet.size());
            }
        };

        assertTrueEventually(task, 10);
    }

    private void assertEntrySetSizeEventually(final int expectedSize, final Predicate predicate,
                                              final QueryCache<Integer, Employee> cache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                int size = cache.size();
                Set<Map.Entry<Integer, Employee>> entries = cache.entrySet(predicate);
                assertEquals("cache size = " + size, expectedSize, entries.size());
            }
        };

        assertTrueEventually(task);
    }

    private void assertValuesSizeEventually(final int expectedSize, final Predicate predicate,
                                            final QueryCache<Integer, Employee> cache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                int size = cache.size();
                Collection<Employee> values = cache.values(predicate);
                assertEquals("cache size = " + size, expectedSize, values.size());
            }
        };

        assertTrueEventually(task, 10);
    }
}
