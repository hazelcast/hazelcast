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

package com.hazelcast.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.lessEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractMapQueryPartitionIterableTest extends HazelcastTestSupport {

    protected TestHazelcastFactory factory;
    protected HazelcastInstance instanceProxy;

    @After
    public void teardown() {
        factory.terminateAll();
    }

    protected abstract <K, V, R> Iterable<R> getIterable(
            IMap<K, V> map,
            int fetchSize,
            int partitionId,
            Projection<Entry<K, V>, R> projection,
            Predicate<K, V> predicate
    );


    @Test(expected = NoSuchElementException.class)
    public void test_next_Throws_Exception_On_EmptyPartition() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        getIterable(map, 10, 1,
                new TestProjection(), Predicates.alwaysTrue()).iterator().next();
    }

    @Test(expected = NullPointerException.class)
    public void test_null_projection_throws_exception() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        getIterable(map, 10, 1, null, Predicates.alwaysTrue());
    }

    @Test(expected = NullPointerException.class)
    public void test_null_predicate_throws_exception() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        getIterable(map, 10, 1, new TestProjection(), null);
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        Iterator<String> iterator = getIterable(map, 10, 1,
                new TestProjection(), Predicates.alwaysTrue()).iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() {
        String value = randomString();
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        fillMap(map, 1, 1, value);

        Iterator<String> iterator = getIterable(map, 10, 1,
                new GetValueProjection<>(), Predicates.alwaysTrue()).iterator();
        String next = iterator.next();
        assertEquals(value, next);
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition_and_HasNext_Returns_False_when_Item_Consumed() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());

        String value = randomString();
        fillMap(map, 1, 1, value);

        Iterator<String> iterator = getIterable(map, 10, 1,
                new GetValueProjection<>(), Predicates.alwaysTrue()).iterator();
        String next = iterator.next();
        assertEquals(value, next);
        boolean hasNext = iterator.hasNext();
        assertFalse(hasNext);
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        fillMap(map, 1, 1, randomString());

        Iterator<String> iterator = getIterable(map, 10, 1,
                new TestProjection(), Predicates.alwaysTrue()).iterator();
        assertTrue(iterator.hasNext());
    }


    @Test
    public void test_with_projection_and_true_predicate() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        fillMap(map, 1, 100, randomString());
        Iterator<String> iterator = getIterable(map, 10, 1,
                new TestProjection(), Predicates.alwaysTrue()).iterator();
        ArrayList<String> projected = collectAll(iterator);
        Collection<String> actualValues = map.values();
        assertEquals(actualValues.size(), projected.size());

        for (String value : actualValues) {
            assertTrue(projected.contains("dummy" + value));
        }
    }

    @Test
    public void test_with_projection_and_predicate() {
        IMap<String, Integer> intMap = instanceProxy.getMap(randomMapName());
        fillMap(intMap, 1, 100);
        Iterator<Entry<String, Integer>> iterator =
                getIterable(intMap, 10, 1, Projections.identity(), new EvenPredicate()).iterator();
        ArrayList<Entry<String, Integer>> projected = collectAll(iterator);

        for (Entry<String, Integer> i : projected) {
            assertEquals(0, i.getValue() % 2);
        }
        Collection<Integer> actualValues = intMap.values();
        assertEquals(actualValues.size() / 2, projected.size());

        for (Entry<String, Integer> e : intMap.entrySet()) {
            if (e.getValue() % 2 == 0) {
                assertTrue(projected.contains(e));
            }
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove_Throws_Exception() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        Iterator<String> iterator = getIterable(map, 10, 1,
                new TestProjection(), Predicates.alwaysTrue()).iterator();
        iterator.remove();
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() {
        IMap<String, String> map = instanceProxy.getMap(randomMapName());
        String value = randomString();
        fillMap(map, 1, 100, value);
        Iterator<String> iterator = getIterable(map, 10, 1,
                new GetValueProjection<>(), Predicates.alwaysTrue()).iterator();

        for (int i = 0; i < 100; i++) {
            String val = iterator.next();
            assertEquals(value, val);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_NoExceptions_When_IndexesAreAccessed_During_PredicateOptimization() {
        int count = instanceProxy.getPartitionService().getPartitions().size() * 10;

        IMap<Integer, Integer> map = instanceProxy.getMap(randomMapName());
        for (int i = 0; i < count; ++i) {
            map.put(i, i);
        }

        // this predicate is a subject for optimizing it into a between predicate
        Predicate<Integer, Integer> predicate = and(greaterEqual("this", 0), lessEqual("this", count - 1));

        Collection result = collectAll(getIterable(map, 10, 1, Projections.identity(), predicate).iterator());
        assertFalse(result.isEmpty());
    }

    private void fillMap(IMap<String, String> map, int partitionId, int count, String value) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(instanceProxy, partitionId);
            map.put(key, value);
        }
    }

    private void fillMap(IMap<String, Integer> map, int partitionId, int count) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(instanceProxy, partitionId);
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
        public String transform(Entry<String, String> input) {
            return "dummy" + input.getValue();
        }
    }

    private static class GetValueProjection<T> implements Projection<Entry<String, T>, T> {
        @Override
        public T transform(Entry<String, T> input) {
            return input.getValue();
        }
    }
}
