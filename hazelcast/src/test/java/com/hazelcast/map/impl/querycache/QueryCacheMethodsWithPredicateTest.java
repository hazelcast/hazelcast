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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.utils.Employee;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheMethodsWithPredicateTest extends AbstractQueryCacheTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> TRUE_PREDICATE = Predicates.alwaysTrue();

    @Parameters(name = "inMemoryFormat: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY}, {OBJECT},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Override
    InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    @Test
    public void testKeySet_onIndexedField() {
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);
        int count = 111;
        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        populateMap(map, count, 2 * count);

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void testKeySet_onIndexedField_whenIncludeValueFalse() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "__key");

        populateMap(map, count, 2 * count);

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("__key >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void testKeySet_onIndexedField_afterRemovalOfSomeIndexes() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        populateMap(map, 17, count);

        // just choose arbitrary numbers to prove whether #keySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("id < " + smallerThan), cache);
    }

    @Test
    public void testKeySetIsNotBackedByQueryCache() {
        int count = 111;
        IMap<Employee, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        for (int i = 0; i < count; i++) {
            map.put(new Employee(i), new Employee(i));
        }

        Predicate<Employee, Employee> predicate = Predicates.lessThan("id", count);
        QueryCache<Employee, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        for (Map.Entry<Employee, Employee> entry : cache.entrySet(predicate)) {
            entry.getValue().setAge(Employee.MAX_AGE + 1);
        }

        for (Map.Entry<Employee, Employee> entry : cache.entrySet(predicate)) {
            assertNotEquals(Employee.MAX_AGE + 1, entry.getValue().getAge());
        }
    }

    @Test
    public void testKeySetIsNotBackedByQueryCache_nonIndexedAttribute() {
        Assume.assumeTrue(inMemoryFormat != OBJECT);
        int count = 111;
        IMap<Employee, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        for (int i = 0; i < count; i++) {
            map.put(new Employee(i), new Employee(i));
        }

        Predicate<Employee, Employee> predicate = Predicates.lessThan("salary", Employee.MAX_SALARY);
        QueryCache<Employee, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        for (Map.Entry<Employee, Employee> entry : cache.entrySet(predicate)) {
            entry.getValue().setAge(Employee.MAX_AGE + 1);
        }

        for (Map.Entry<Employee, Employee> entry : cache.entrySet(predicate)) {
            assertNotEquals(Employee.MAX_AGE + 1, entry.getValue().getAge());
        }
    }

    @Test
    public void testEntrySet() {
        int count = 1;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        populateMap(map, count, 2 * count);

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 0;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertEntrySetSizeEventually(expectedSize, Predicates.sql("id >= " + equalsOrBiggerThan), cache);
    }


    @Test
    public void testEntrySetIsNotBackedByQueryCache() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        Predicate<Integer, Employee> predicate = Predicates.lessThan("id", count);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        for (Map.Entry<Integer, Employee> entry : cache.entrySet(predicate)) {
            entry.getValue().setAge(Employee.MAX_AGE + 1);
        }

        for (Map.Entry<Integer, Employee> entry : cache.entrySet(predicate)) {
            assertNotEquals(Employee.MAX_AGE + 1, entry.getValue().getAge());
        }
    }

    @Test
    public void testEntrySetIsNotBackedByQueryCache_nonIndexedAttribute() {
        Assume.assumeTrue(inMemoryFormat != OBJECT);

        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        Predicate<Integer, Employee> predicate = Predicates.lessThan("salary", Employee.MAX_SALARY);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        for (Map.Entry<Integer, Employee> entry : cache.entrySet(predicate)) {
            entry.getValue().setAge(Employee.MAX_AGE + 1);
        }

        for (Map.Entry<Integer, Employee> entry : cache.entrySet(predicate)) {
            assertNotEquals(Employee.MAX_AGE + 1, entry.getValue().getAge());
        }
    }

    @Test
    public void testEntrySet_whenIncludeValueFalse() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        cache.addIndex(IndexType.SORTED, "id");

        removeEntriesFromMap(map, 17, count);

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 0;
        assertEntrySetSizeEventually(expectedSize, Predicates.sql("id < " + smallerThan), cache);
    }

    @Test
    public void testEntrySet_withIndexedKeys_whenIncludeValueFalse() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        // here adding key index. (key --> integer; value --> Employee)
        cache.addIndex(IndexType.SORTED, "__key");

        removeEntriesFromMap(map, 17, count);

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertEntrySetSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
    }

    @Test
    public void testValues() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        populateMap(map, count, 2 * count);

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertValuesSizeEventually(expectedSize, Predicates.sql("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void testValuesAreNotBackedByQueryCache() {
        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        Predicate<Integer, Employee> predicate = Predicates.lessThan("id", count);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        for (Employee employee : cache.values(predicate)) {
            employee.setAge(Employee.MAX_AGE + 1);
        }

        for (Employee employee : cache.values(predicate)) {
            assertNotEquals(Employee.MAX_AGE + 1, employee.getAge());
        }
    }

    @Test
    public void testValuesAreNotBackedByQueryCache_nonIndexedAttribute() {
        Assume.assumeTrue(inMemoryFormat != OBJECT);

        int count = 111;
        IMap<Integer, Employee> map = getIMapWithDefaultConfig(TRUE_PREDICATE);

        populateMap(map, count);

        Predicate<Integer, Employee> predicate = Predicates.lessThan("salary", Employee.MAX_SALARY);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);
        cache.addIndex(IndexType.SORTED, "id");

        for (Employee employee : cache.values(predicate)) {
            employee.setAge(Employee.MAX_AGE + 1);
        }

        for (Employee employee : cache.values(predicate)) {
            assertNotEquals(Employee.MAX_AGE + 1, employee.getAge());
        }
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
        assertValuesSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
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
        assertValuesSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
    }

    private void assertKeySetSizeEventually(final int expectedSize, final Predicate predicate,
                                            final QueryCache<Integer, Employee> cache) {
        AssertTask task = () -> {
            int size = cache.size();
            Set<Integer> keySet = cache.keySet(predicate);
            assertEquals("cache size = " + size, expectedSize, keySet.size());
        };

        assertTrueEventually(task, 10);
    }

    private void assertEntrySetSizeEventually(final int expectedSize, final Predicate predicate,
                                              final QueryCache<Integer, Employee> cache) {
        AssertTask task = () -> {
            int size = cache.size();
            Set<Map.Entry<Integer, Employee>> entries = cache.entrySet(predicate);
            assertEquals("cache size = " + size, expectedSize, entries.size());
        };

        assertTrueEventually(task);
    }

    private void assertValuesSizeEventually(final int expectedSize, final Predicate predicate,
                                            final QueryCache<Integer, Employee> cache) {
        AssertTask task = () -> {
            int size = cache.size();
            Collection<Employee> values = cache.values(predicate);
            assertEquals("cache size = " + size, expectedSize, values.size());
        };

        assertTrueEventually(task, 10);
    }
}
