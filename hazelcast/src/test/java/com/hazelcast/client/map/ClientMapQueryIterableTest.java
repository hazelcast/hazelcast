/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapQueryIterableTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory;
    private HazelcastInstance server;
    private HazelcastInstance client;

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
        server = factory.newHazelcastInstance(getConfig());
        client = factory.newHazelcastClient(getClientConfig());
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test(expected = NoSuchElementException.class)
    public void test_next_Throws_Exception_On_EmptyMap() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        proxy.iterable(10, new TestProjection(), Predicates.alwaysTrue()).iterator().next();
    }

    @Test(expected = NullPointerException.class)
    public void test_null_projection_throws_exception() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        proxy.iterable(10, null, Predicates.alwaysTrue());
    }

    @Test(expected = NullPointerException.class)
    public void test_null_predicate_throws_exception() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        proxy.iterable(10, new TestProjection(), null);
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyMap() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        final Iterator<String> iterator = proxy.iterable(10, new TestProjection(), Predicates.alwaysTrue()).iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyMap() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        proxy.put(key, value);

        final Iterator<String> iterator = proxy.iterable(10, new GetValueProjection<>(), Predicates.alwaysTrue()).iterator();
        final String next = iterator.next();
        assertEquals(value, next);
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyMap_and_HasNext_Returns_False_when_Item_Consumed() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        proxy.put(key, value);

        final Iterator<String> iterator = proxy.iterable(10, new GetValueProjection<>(), Predicates.alwaysTrue()).iterator();
        final String next = iterator.next();
        assertEquals(value, next);
        boolean hasNext = iterator.hasNext();
        assertFalse(hasNext);
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyMap() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        final String key = generateKeyForPartition(server, 1);
        final String value = randomString();
        proxy.put(key, value);

        final Iterator<String> iterator = proxy.iterable(10, new TestProjection(), Predicates.alwaysTrue()).iterator();
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_with_projection_and_true_predicate() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        for (int i = 0; i < 100; i++) {
            String key = generateKeyForPartition(server, 1);
            proxy.put(key, randomString());
        }
        final Iterator<String> iterator = proxy.iterable(10, new TestProjection(), Predicates.alwaysTrue()).iterator();

        final ArrayList<String> projected = collectAll(iterator);

        final Collection<String> actualValues = proxy.values();
        assertEquals(actualValues.size(), projected.size());

        for (String value : actualValues) {
            assertTrue(projected.contains("dummy" + value));
        }
    }

    @Test
    public void test_with_projection_and_predicate() {
        final ClientMapProxy<String, Integer> intMap = getMapProxy();

        for (int i = 0; i < 100; i++) {
            String key = generateKeyForPartition(server, 1);
            intMap.put(key, i);
        }

        final Iterator<Entry<String, Integer>> iterator =
                intMap.iterable(10, Projections.identity(), new EvenPredicate()).iterator();

        final ArrayList<Entry<String, Integer>> projected = collectAll(iterator);
        for (Entry<String, Integer> i : projected) {
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
        final ClientMapProxy<String, String> proxy = getMapProxy();
        final Iterator<String> iterator = proxy.iterable(10, new TestProjection(), Predicates.alwaysTrue()).iterator();

        iterator.remove();
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() {
        final ClientMapProxy<String, String> proxy = getMapProxy();
        String value = randomString();
        for (int i = 0; i < 100; i++) {
            String key = generateKeyForPartition(server, 1);
            proxy.put(key, value);
        }
        final Iterator<String> iterator = proxy.iterable(10, new GetValueProjection<>(), Predicates.alwaysTrue()).iterator();
        for (int i = 0; i < 100; i++) {
            String val = iterator.next();
            assertEquals(value, val);
        }
    }

    private <T> ArrayList<T> collectAll(Iterator<T> iterator) {
        final ArrayList<T> projected = new ArrayList<>();
        while (iterator.hasNext()) {
            projected.add(iterator.next());
        }
        return projected;
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> ClientMapProxy<K, V> getMapProxy() {
        String mapName = randomString();
        return (ClientMapProxy<K, V>) client.getMap(mapName);
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
