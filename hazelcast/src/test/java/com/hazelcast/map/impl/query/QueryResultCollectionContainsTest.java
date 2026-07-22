/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.map.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for contains() complexity in QueryResultCollection.
 *
 * Issue: https://github.com/hazelcast/hazelcast/issues/26535
 *
 * Root cause: AbstractCollection.contains() iterates all elements via iterator() — O(n).
 * QueryResultCollection already stores rows in a HashSet (when distinct=true, i.e. keySet/entrySet),
 * so contains() can be O(1) by serializing the query object and using HashSet.contains() directly.
 *
 * Tests named *_doesNotUseIterator are "red" tests:
 *   they FAIL before the fix and PASS after.
 * Tests named *_correctness verify correct boolean results (pass both before and after).
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryResultCollectionContainsTest extends HazelcastTestSupport {

    private static final int ENTRY_COUNT = 100;

    private InternalSerializationService ss;
    private IMap<String, String> map;
    // stored so multi-node tests can add more instances to the same factory
    private TestHazelcastInstanceFactory nodeFactory;

    @Before
    public void setUp() {
        ss = new DefaultSerializationServiceBuilder().build();
        // capacity 3: 1 for single-node tests + 2 slots for multi-node tests
        nodeFactory = createHazelcastInstanceFactory(3);
        map = nodeFactory.newHazelcastInstance(regularInstanceConfig()).getMap(randomName());
    }

    // -----------------------------------------------------------------------
    // Unit tests — no HazelcastInstance, construct QueryResultCollection directly
    // -----------------------------------------------------------------------

    /**
     * RED test: currently FAILS because AbstractCollection.contains() calls iterator()
     * which walks all N rows. After the fix, contains() must NOT use iterator() at all —
     * it should delegate directly to the underlying HashSet via a serialized probe row.
     */
    @Test
    public void keySet_contains_doesNotUseIterator() {
        CountingQueryResultCollection<String> counting = countingKeyCollection(ENTRY_COUNT);

        counting.contains("key42");

        assertEquals(
                "contains() must not call iterator() — expected O(1) HashSet lookup",
                0, counting.iteratorCallCount);
    }

    /**
     * RED test: same as above for ENTRY mode.
     * AbstractCollection.contains() iterates all rows — iterator() is called.
     * After the fix, iterator() must not be called.
     */
    @Test
    public void entrySet_contains_doesNotUseIterator() {
        CountingQueryResultCollection<Map.Entry<String, String>> counting = countingEntryCollection(ENTRY_COUNT);

        counting.contains(new AbstractMap.SimpleEntry<>("key42", "value42"));

        assertEquals(
                "contains() must not call iterator() — expected O(1) HashSet lookup",
                0, counting.iteratorCallCount);
    }

    @Test
    public void keySet_contains_existingKey_correctness() {
        QueryResultCollection<String> collection = buildKeyCollection(ENTRY_COUNT);

        assertTrue(collection.contains("key0"));
        assertTrue(collection.contains("key42"));
        assertTrue(collection.contains("key99"));
    }

    @Test
    public void keySet_contains_nonExistingKey_correctness() {
        QueryResultCollection<String> collection = buildKeyCollection(ENTRY_COUNT);

        assertFalse(collection.contains("missing"));
        assertFalse(collection.contains("key100"));
    }

    @Test
    public void entrySet_contains_existingEntry_correctness() {
        QueryResultCollection<Map.Entry<String, String>> collection = buildEntryCollection(ENTRY_COUNT);

        assertTrue(collection.contains(new AbstractMap.SimpleEntry<>("key7", "value7")));
    }

    @Test
    public void entrySet_contains_keyExistsButWrongValue_returnsFalse() {
        QueryResultCollection<Map.Entry<String, String>> collection = buildEntryCollection(ENTRY_COUNT);

        assertFalse(collection.contains(new AbstractMap.SimpleEntry<>("key7", "wrongValue")));
    }

    @Test
    public void entrySet_contains_nonExistingEntry_correctness() {
        QueryResultCollection<Map.Entry<String, String>> collection = buildEntryCollection(ENTRY_COUNT);

        assertFalse(collection.contains(new AbstractMap.SimpleEntry<>("missing", "value")));
    }

    // -----------------------------------------------------------------------
    // Integration tests — real IMap
    // -----------------------------------------------------------------------

    @Test
    public void imap_keySet_contains_existingKey() {
        map.put("alpha", "1");
        map.put("beta", "2");
        map.put("gamma", "3");

        Set<String> keySet = map.keySet();

        assertTrue(keySet.contains("alpha"));
        assertTrue(keySet.contains("beta"));
        assertTrue(keySet.contains("gamma"));
    }

    @Test
    public void imap_keySet_contains_nonExistingKey() {
        map.put("alpha", "1");

        Set<String> keySet = map.keySet();

        assertFalse(keySet.contains("missing"));
        assertFalse(keySet.contains("ALPHA"));
    }

    @Test
    public void imap_keySet_withPredicate_contains() {
        for (int i = 0; i < 20; i++) {
            map.put("key" + i, i < 10 ? "good" : "bad");
        }

        Set<String> filteredKeySet = map.keySet(Predicates.equal("this", "good"));

        for (int i = 0; i < 10; i++) {
            assertTrue("key" + i + " should be in filtered keySet", filteredKeySet.contains("key" + i));
        }
        for (int i = 10; i < 20; i++) {
            assertFalse("key" + i + " must not be in filtered keySet", filteredKeySet.contains("key" + i));
        }
    }

    @Test
    public void imap_entrySet_contains_existingEntry() {
        map.put("k1", "v1");
        map.put("k2", "v2");

        Set<Map.Entry<String, String>> entrySet = map.entrySet();

        assertTrue(entrySet.contains(new AbstractMap.SimpleEntry<>("k1", "v1")));
        assertTrue(entrySet.contains(new AbstractMap.SimpleEntry<>("k2", "v2")));
        assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<>("k1", "wrongValue")));
        assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<>("missing", "v1")));
    }

    @Test
    public void imap_keySet_empty_contains_returnsFalse() {
        assertFalse(map.keySet().contains("anything"));
    }

    // -----------------------------------------------------------------------
    // Integration tests — ResultSet path (PagingPredicate)
    // -----------------------------------------------------------------------

    /**
     * keySet(PagingPredicate) returns ResultSet, not QueryResultCollection.
     * Verifies our second fix works end-to-end through IMap.
     */
    @Test
    public void imap_keySet_pagingPredicate_contains_existingKey() {
        for (int i = 0; i < 20; i++) {
            map.put("key" + i, "value" + i);
        }
        PagingPredicate<String, String> paging = Predicates.pagingPredicate(10);

        Set<String> keySet = map.keySet(paging);

        for (String key : keySet) {
            assertTrue("keySet(PagingPredicate).contains() must find: " + key, keySet.contains(key));
        }
        assertFalse(keySet.contains("nonexistent"));
    }

    @Test
    public void imap_entrySet_pagingPredicate_contains_existingEntry() {
        for (int i = 0; i < 20; i++) {
            map.put("key" + i, "value" + i);
        }
        PagingPredicate<String, String> paging = Predicates.pagingPredicate(10);

        Set<Map.Entry<String, String>> entrySet = map.entrySet(paging);

        for (Map.Entry<String, String> entry : entrySet) {
            assertTrue("entrySet(PagingPredicate).contains() must find: " + entry,
                    entrySet.contains(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue())));
        }
    }

    // -----------------------------------------------------------------------
    // Integration tests — values() via IMap
    // -----------------------------------------------------------------------

    @Test
    public void imap_values_contains_existingValue() {
        map.put("k1", "alpha");
        map.put("k2", "beta");
        map.put("k3", "alpha"); // дубль значения — values() их не дедуплицирует

        Collection<String> values = map.values();

        assertTrue(values.contains("alpha"));
        assertTrue(values.contains("beta"));
        assertFalse(values.contains("missing"));
    }

    // -----------------------------------------------------------------------
    // Integration tests — localKeySet()
    // -----------------------------------------------------------------------

    @Test
    public void imap_localKeySet_contains_existingKey() {
        map.put("k1", "v1");
        map.put("k2", "v2");

        Set<String> localKeys = map.localKeySet();

        // localKeySet() returns only keys owned by this node — verify contains() works
        for (String key : localKeys) {
            assertTrue("localKeySet().contains() must find: " + key, localKeys.contains(key));
        }
        assertFalse(localKeys.contains("definitelyMissing"));
    }

    // -----------------------------------------------------------------------
    // Integration tests — multi-node cluster
    // -----------------------------------------------------------------------

    /**
     * Data is distributed across 2 nodes. keySet() collects results from both nodes,
     * deduplicates into HashSet, then contains() must work correctly.
     * This verifies the List→HashSet deduplication + our O(1) fix together.
     */
    @Test
    public void imap_multiNode_keySet_contains_existingKey() {
        // add second node to the same factory — data distributes across both
        nodeFactory.newHazelcastInstance(regularInstanceConfig());
        String mapName = randomName();
        IMap<String, String> distributedMap = nodeFactory.getAllHazelcastInstances()
                .iterator().next().getMap(mapName);

        for (int i = 0; i < 50; i++) {
            distributedMap.put("key" + i, "value" + i);
        }

        Set<String> keySet = distributedMap.keySet();

        assertEquals(50, keySet.size());
        for (int i = 0; i < 50; i++) {
            assertTrue("key" + i + " must be found in multi-node keySet",
                    keySet.contains("key" + i));
        }
        assertFalse(keySet.contains("key50"));
    }

    @Test
    public void imap_multiNode_entrySet_contains_existingEntry() {
        // add second node to the same factory — data distributes across both
        nodeFactory.newHazelcastInstance(regularInstanceConfig());
        String mapName = randomName();
        IMap<String, String> distributedMap = nodeFactory.getAllHazelcastInstances()
                .iterator().next().getMap(mapName);

        distributedMap.put("k1", "v1");
        distributedMap.put("k2", "v2");
        distributedMap.put("k3", "v3");

        Set<Map.Entry<String, String>> entrySet = distributedMap.entrySet();

        assertTrue(entrySet.contains(new AbstractMap.SimpleEntry<>("k1", "v1")));
        assertTrue(entrySet.contains(new AbstractMap.SimpleEntry<>("k2", "v2")));
        assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<>("k1", "wrongValue")));
        assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<>("missing", "v1")));
    }

    // -----------------------------------------------------------------------
    // Timing benchmark — run manually to measure contains() throughput
    // -----------------------------------------------------------------------

    /**
     * Measures contains() throughput for increasing collection sizes.
     * Does NOT assert on timing (results are non-deterministic), just logs them.
     *
     * Expected output after the fix:
     *   ns/op should be roughly CONSTANT across all sizes → O(1)
     *
     * To compare with O(n) behaviour, temporarily revert QueryResultCollection.contains()
     * to "return super.contains(o)" and re-run — ns/op will grow linearly with size.
     */
    @Test
    public void keySet_contains_timingBenchmark() {
        Logger log = Logger.getLogger(getClass().getName());
        int[] sizes = {100, 1_000, 10_000, 100_000};
        int lookups = 500;
        StringBuilder sb = new StringBuilder("\nkeySet.contains() timing benchmark\n");
        sb.append(String.format("%-12s  %-14s  %-10s%n", "Size", "Total (ms)", "ns/op"));
        sb.append("-".repeat(42)).append("\n");

        for (int size : sizes) {
            QueryResultCollection<String> collection = buildKeyCollection(size);
            // warm-up
            for (int i = 0; i < 50; i++) {
                collection.contains("key" + (i % size));
            }
            long start = System.nanoTime();
            for (int i = 0; i < lookups; i++) {
                collection.contains("key" + (i % size));
            }
            long elapsed = System.nanoTime() - start;
            sb.append(String.format("%-12d  %-14.3f  %-10.1f%n",
                    size, elapsed / 1e6, (double) elapsed / lookups));
        }
        log.info(sb.toString());
    }

    // -----------------------------------------------------------------------
    // Helpers: build collections directly without HazelcastInstance
    // -----------------------------------------------------------------------

    /**
     * KEY mode: each row has key=Data("keyN"), value=null.
     * This mirrors server behavior — in KEY queries the server does not send values.
     */
    private QueryResultCollection<String> buildKeyCollection(int count) {
        QueryResult queryResult = new QueryResult();
        for (int i = 0; i < count; i++) {
            queryResult.addRow(new QueryResultRow(ss.toData("key" + i), null));
        }
        return new QueryResultCollection<>(ss, IterationType.KEY, false, true, queryResult);
    }

    /** ENTRY mode: each row has key=Data("keyN"), value=Data("valueN"). */
    private QueryResultCollection<Map.Entry<String, String>> buildEntryCollection(int count) {
        QueryResult queryResult = new QueryResult();
        for (int i = 0; i < count; i++) {
            Data key = ss.toData("key" + i);
            Data value = ss.toData("value" + i);
            queryResult.addRow(new QueryResultRow(key, value));
        }
        return new QueryResultCollection<>(ss, IterationType.ENTRY, false, true, queryResult);
    }

    private CountingQueryResultCollection<String> countingKeyCollection(int count) {
        return new CountingQueryResultCollection<>(buildKeyCollection(count));
    }

    private CountingQueryResultCollection<Map.Entry<String, String>> countingEntryCollection(int count) {
        return new CountingQueryResultCollection<>(buildEntryCollection(count));
    }

    // -----------------------------------------------------------------------
    // CountingQueryResultCollection — tracks iterator() calls to prove O(n)
    // -----------------------------------------------------------------------

    /**
     * Wraps a QueryResultCollection to count how many times iterator() is invoked.
     *
     * AbstractCollection.contains() always calls iterator() — so if contains() delegates
     * to AbstractCollection (current O(n) path), iteratorCallCount will be 1 after one
     * contains() call. After the fix, contains() skips iterator() entirely: iteratorCallCount == 0.
     */
    private static class CountingQueryResultCollection<E> extends QueryResultCollection<E> {

        int iteratorCallCount;
        int nextCallCount;

        CountingQueryResultCollection(QueryResultCollection<E> source) {
            super(getField(source, "serializationService"),
                    getField(source, "iterationType"),
                    getField(source, "binary"),
                    false,
                    new QueryResult());
            // replace the empty rows list with the HashSet from the original collection
            setField(this, "rows", getField(source, "rows"));
        }

        @Override
        public Iterator<E> iterator() {
            iteratorCallCount++;
            Iterator<E> delegate = super.iterator();
            return new Iterator<E>() {
                @Override public boolean hasNext() { return delegate.hasNext(); }
                @Override public E next() { nextCallCount++; return delegate.next(); }
                @Override public void remove() { delegate.remove(); }
            };
        }

        @SuppressWarnings("unchecked")
        private static <T> T getField(Object obj, String name) {
            try {
                var f = QueryResultCollection.class.getDeclaredField(name);
                f.setAccessible(true);
                return (T) f.get(obj);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Cannot access field: " + name, e);
            }
        }

        private static void setField(Object obj, String name, Object value) {
            try {
                var f = QueryResultCollection.class.getDeclaredField(name);
                f.setAccessible(true);
                f.set(obj, value);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Cannot set field: " + name, e);
            }
        }
    }
}
