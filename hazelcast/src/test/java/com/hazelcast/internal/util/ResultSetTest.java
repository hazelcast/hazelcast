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

package com.hazelcast.internal.util;

import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class ResultSetTest {

    @Test
    public void testSize_whenEmpty() {
        List<Map.Entry> emptyList = Collections.emptyList();
        ResultSet resultSet = new ResultSet(emptyList, IterationType.KEY);
        assertEquals(0, resultSet.size());
    }

    @Test
    public void testSize_whenNull() {
        ResultSet resultSet = new ResultSet(null, IterationType.KEY);
        assertEquals(0, resultSet.size());
    }

    @Test
    public void testSize_whenNotEmpty() {
        List<Map.Entry> entries = new ArrayList<>();
        entries.add(new MapEntrySimple(null, null));
        ResultSet resultSet = new ResultSet(entries, IterationType.KEY);
        assertEquals(1, resultSet.size());
    }

    @Test
    public void testIterator_whenEmpty() {
        List<Map.Entry> emptyList = Collections.emptyList();
        ResultSet resultSet = new ResultSet(emptyList, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testIterator_whenNull() {
        ResultSet resultSet = new ResultSet(null, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Key() {
        List<Map.Entry> entries = new ArrayList<>();
        MapEntrySimple entry = new MapEntrySimple("key", "value");
        entries.add(entry);
        ResultSet resultSet = new ResultSet(entries, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("key", iterator.next());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Value() {
        List<Map.Entry> entries = new ArrayList<>();
        MapEntrySimple entry = new MapEntrySimple("key", "value");
        entries.add(entry);
        ResultSet resultSet = new ResultSet(entries, IterationType.VALUE);
        Iterator iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("value", iterator.next());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Entry() {
        List<Map.Entry> entries = new ArrayList<>();
        MapEntrySimple entry = new MapEntrySimple("key", "value");
        entries.add(entry);
        ResultSet resultSet = new ResultSet(entries, IterationType.ENTRY);
        Iterator<Map.Entry> iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        Map.Entry entryFromIterator = iterator.next();
        assertEquals("key", entryFromIterator.getKey());
        assertEquals("value", entryFromIterator.getValue());
    }

    // -----------------------------------------------------------------------
    // contains() — correctness
    // -----------------------------------------------------------------------

    @Test
    public void testContains_key_whenExists_returnsTrue() {
        ResultSet resultSet = new ResultSet(entries("a", "1", "b", "2"), IterationType.KEY);
        assertTrue(resultSet.contains("a"));
        assertTrue(resultSet.contains("b"));
    }

    @Test
    public void testContains_key_whenMissing_returnsFalse() {
        ResultSet resultSet = new ResultSet(entries("a", "1"), IterationType.KEY);
        assertFalse(resultSet.contains("missing"));
    }

    @Test
    public void testContains_value_whenExists_returnsTrue() {
        ResultSet resultSet = new ResultSet(entries("a", "1", "b", "2"), IterationType.VALUE);
        assertTrue(resultSet.contains("1"));
        assertTrue(resultSet.contains("2"));
    }

    @Test
    public void testContains_value_whenMissing_returnsFalse() {
        ResultSet resultSet = new ResultSet(entries("a", "1"), IterationType.VALUE);
        assertFalse(resultSet.contains("99"));
    }

    @Test
    public void testContains_entry_whenExists_returnsTrue() {
        ResultSet resultSet = new ResultSet(entries("a", "1", "b", "2"), IterationType.ENTRY);
        assertTrue(resultSet.contains(new MapEntrySimple("a", "1")));
    }

    @Test
    public void testContains_entry_whenKeyMatchesButValueDiffers_returnsFalse() {
        ResultSet resultSet = new ResultSet(entries("a", "1"), IterationType.ENTRY);
        assertFalse(resultSet.contains(new MapEntrySimple("a", "wrong")));
    }

    @Test
    public void testContains_whenNull_returnsFalse() {
        ResultSet resultSet = new ResultSet(null, IterationType.KEY);
        assertFalse(resultSet.contains("anything"));
    }

    @Test
    public void testContains_whenEmpty_returnsFalse() {
        ResultSet resultSet = new ResultSet(Collections.emptyList(), IterationType.KEY);
        assertFalse(resultSet.contains("anything"));
    }

    // -----------------------------------------------------------------------
    // contains() — must NOT use iterator after the fix (O(1) via lazy HashSet)
    // -----------------------------------------------------------------------

    /**
     * RED test for IterationType.KEY: currently FAILS because AbstractCollection.contains()
     * calls iterator() → O(n). After the fix, contains() uses a lazy HashSet → iterator() not called.
     */
    @Test
    public void testContains_doesNotUseIterator_key() {
        CountingResultSet resultSet = new CountingResultSet(entries("a", "1", "b", "2", "c", "3"), IterationType.KEY);

        resultSet.contains("b");

        assertEquals("contains() must not call iterator() — expected O(1) HashSet lookup",
                0, resultSet.iteratorCallCount);
    }

    /**
     * RED test for IterationType.VALUE.
     */
    @Test
    public void testContains_doesNotUseIterator_value() {
        CountingResultSet resultSet = new CountingResultSet(entries("a", "1", "b", "2", "c", "3"), IterationType.VALUE);

        resultSet.contains("2");

        assertEquals("contains() must not call iterator() — expected O(1) HashSet lookup",
                0, resultSet.iteratorCallCount);
    }

    /**
     * RED test for IterationType.ENTRY.
     */
    @Test
    public void testContains_doesNotUseIterator_entry() {
        CountingResultSet resultSet = new CountingResultSet(entries("a", "1", "b", "2", "c", "3"), IterationType.ENTRY);

        resultSet.contains(new MapEntrySimple("b", "2"));

        assertEquals("contains() must not call iterator() — expected O(1) HashSet lookup",
                0, resultSet.iteratorCallCount);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Builds a List<Map.Entry> from alternating key/value strings. */
    @SuppressWarnings("unchecked")
    private static List<Map.Entry> entries(String... keyValues) {
        List<Map.Entry> list = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            list.add(new MapEntrySimple(keyValues[i], keyValues[i + 1]));
        }
        return list;
    }

    /**
     * Subclass of ResultSet that counts iterator() calls.
     * If contains() delegates to AbstractCollection (O(n) path), iteratorCallCount > 0.
     * After the fix, contains() uses the lazy HashSet: iteratorCallCount == 0.
     */
    private static class CountingResultSet extends ResultSet {

        int iteratorCallCount;

        CountingResultSet(List<Map.Entry> entries, IterationType iterationType) {
            super(entries, iterationType);
        }

        @Override
        public Iterator iterator() {
            iteratorCallCount++;
            return super.iterator();
        }
    }
}
