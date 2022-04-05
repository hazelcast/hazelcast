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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.utils.Employee;
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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheMethodsWithPredicateTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> TRUE_PREDICATE = Predicates.alwaysTrue();

    private static final int DEFAULT_TEST_TIMEOUT = 120;

    private static TestHazelcastFactory factory = new TestHazelcastFactory();

    @BeforeClass
    public static void setUp() {
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();
    }

    @AfterClass
    public static void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void test_keySet() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        // populate map before construction of query cache.
        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addIndex(IndexType.SORTED, "id");

        // populate map after construction of query cache
        for (int i = count; i < 2 * count; i++) {
            map.put(i, new Employee(i));
        }

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void test_keySet_whenIncludeValueFalse() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        // populate map before construction of query cache
        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        cache.addIndex(IndexType.SORTED, "__key");

        // populate map after construction of query cache
        for (int i = count; i < 2 * count; i++) {
            map.put(i, new Employee(i));
        }

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("__key >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void test_keySet_onRemovedIndexes() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addIndex(IndexType.SORTED, "id");

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #keySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("id < " + smallerThan), cache);
    }

    @Test
    public void test_entrySet() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        // populate map before construction of query cache
        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addIndex(IndexType.SORTED, "id");

        // populate map after construction of query cache
        for (int i = count; i < 2 * count; i++) {
            map.put(i, new Employee(i));
        }

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertEntrySetSizeEventually(expectedSize, Predicates.sql("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void test_entrySet_whenIncludeValueFalse() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        cache.addIndex(IndexType.SORTED, "id");

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 0;
        assertEntrySetSizeEventually(expectedSize, Predicates.sql("id < " + smallerThan), cache);
    }

    @Test
    public void test_entrySet_withIndexedKeys_whenIncludeValueFalse() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        // here adding key index. (key --> integer; value --> Employee)
        cache.addIndex(IndexType.SORTED, "__key");

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // ust choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertEntrySetSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
    }

    @Test
    public void test_values() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        // populate map before construction of query cache
        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addIndex(IndexType.SORTED, "id");

        // populate map after construction of query cache
        for (int i = count; i < 2 * count; i++) {
            map.put(i, new Employee(i));
        }

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertValuesSizeEventually(expectedSize, Predicates.sql("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void test_values_withoutIndex() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertValuesSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
    }

    @Test
    public void test_values_withoutIndex_whenIncludeValueFalse() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 0;
        assertValuesSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
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

        assertTrueEventually(task, DEFAULT_TEST_TIMEOUT);
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

        assertTrueEventually(task, DEFAULT_TEST_TIMEOUT);
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

        assertTrueEventually(task, DEFAULT_TEST_TIMEOUT);
    }

    private HazelcastInstance getInstance() {
        return factory.newHazelcastClient();
    }
}
