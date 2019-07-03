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

package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.lessEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapQueryPartitionIteratorTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private MapProxyImpl<String, String> proxy;

    @Before
    public void init() {
        this.instance = createHazelcastInstance();
        this.proxy = (MapProxyImpl<String, String>) instance.<String, String>getMap(randomMapName());
    }

    @Test(expected = NoSuchElementException.class)
    public void test_next_Throws_Exception_On_EmptyPartition() {
        proxy.iterator(10, 1,
                new TestProjection(), Predicates.alwaysTrue()).next();
    }

    @Test(expected = NullPointerException.class)
    public void test_null_projection_throws_exception() {
        proxy.iterator(10, 1, null, Predicates.alwaysTrue());
    }

    @Test(expected = NullPointerException.class)
    public void test_null_predicate_throws_exception() {
        proxy.iterator(10, 1, new TestProjection(), null);
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() {
        final Iterator<String> iterator = proxy.iterator(10, 1,
                new TestProjection(), Predicates.alwaysTrue());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() {
        String value = randomString();
        fillMap(proxy, 1, 1, value);

        final Iterator<String> iterator = proxy.iterator(10, 1,
                new GetValueProjection<>(), Predicates.alwaysTrue());
        final String next = iterator.next();
        assertEquals(value, next);
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition_and_HasNext_Returns_False_when_Item_Consumed() {
        String value = randomString();
        fillMap(proxy, 1, 1, value);

        final Iterator<String> iterator = proxy.iterator(10, 1,
                new GetValueProjection<>(), Predicates.alwaysTrue());
        final String next = iterator.next();
        assertEquals(value, next);
        boolean hasNext = iterator.hasNext();
        assertFalse(hasNext);
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() {
        fillMap(proxy, 1, 1, randomString());

        final Iterator<String> iterator = proxy.iterator(10, 1,
                new TestProjection(), Predicates.alwaysTrue());
        assertTrue(iterator.hasNext());
    }


    @Test
    public void test_with_projection_and_true_predicate() {
        fillMap(proxy, 1, 100, randomString());
        final Iterator<String> iterator = proxy.iterator(10, 1,
                new TestProjection(), Predicates.alwaysTrue());
        final ArrayList<String> projected = collectAll(iterator);

        final Collection<String> actualValues = proxy.values();
        assertEquals(actualValues.size(), projected.size());

        for (String value : actualValues) {
            assertTrue(projected.contains("dummy" + value));
        }
    }

    @Test
    public void test_with_projection_and_predicate() {
        final MapProxyImpl<String, Integer> intMap =
                (MapProxyImpl<String, Integer>) instance.<String, Integer>getMap(randomMapName());
        fillMap(intMap, 1, 100);
        final Iterator<Map.Entry<String, Integer>> iterator = intMap.iterator(10, 1,
                Projections.identity(),
                new EvenPredicate());
        final ArrayList<Map.Entry<String, Integer>> projected = collectAll(iterator);

        for (Map.Entry<String, Integer> i : projected) {
            assertEquals(0, i.getValue() % 2);
        }
        final Collection<Integer> actualValues = intMap.values();
        assertEquals(actualValues.size() / 2, projected.size());

        for (Entry<String, Integer> e : intMap.entrySet()) {
            if (e.getValue() % 2 == 0) {
                assertTrue(projected.contains(e));
            }
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove_Throws_Exception() {
        final Iterator<String> iterator = proxy.iterator(10, 1,
                new TestProjection(), Predicates.alwaysTrue());

        iterator.remove();
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() {
        String value = randomString();
        fillMap(proxy, 1, 100, value);
        final Iterator<String> iterator = proxy.iterator(10, 1,
                new GetValueProjection<>(), Predicates.alwaysTrue());

        for (int i = 0; i < 100; i++) {
            String val = iterator.next();
            assertEquals(value, val);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_NoExceptions_When_IndexesAreAccessed_During_PredicateOptimization() {
        int count = instance.getPartitionService().getPartitions().size() * 10;

        MapProxyImpl<Integer, Integer> map = (MapProxyImpl<Integer, Integer>) instance.<Integer, Integer>getMap(randomMapName());
        for (int i = 0; i < count; ++i) {
            map.put(i, i);
        }

        // this predicate is a subject for optimizing it into a between predicate
        Predicate<Integer, Integer> predicate = and(greaterEqual("this", 0), lessEqual("this", count - 1));

        Collection result = collectAll(map.iterator(10, 1, Projections.identity(), predicate));
        assertFalse(result.isEmpty());
    }

    private void fillMap(IMap<String, String> map, int partitionId, int count, String value) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(instance, partitionId);
            map.put(key, value);
        }
    }

    private void fillMap(IMap<String, Integer> map, int partitionId, int count) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(instance, partitionId);
            map.put(key, i);
        }
    }

    private <T> ArrayList<T> collectAll(Iterator<T> iterator) {
        final ArrayList<T> projected = new ArrayList<>();
        while (iterator.hasNext()) {
            projected.add(iterator.next());
        }
        return projected;
    }

    private static class EvenPredicate implements Predicate<String, Integer> {
        @Override
        public boolean apply(Entry<String, Integer> mapEntry) {
            return mapEntry.getValue() % 2 == 0;
        }
    }

    private static class TestProjection implements Projection<Entry<String, String>, String> {
        @Override
        public String transform(Map.Entry<String, String> input) {
            return "dummy" + input.getValue();
        }
    }

    private static class GetValueProjection<T> implements Projection<Entry<String, T>, T> {
        @Override
        public T transform(Map.Entry<String, T> input) {
            return input.getValue();
        }
    }
}
