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

package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Object2LongHashMapTest {

    public static final long MISSING_VALUE = -1L;

    private Object2LongHashMap<String> map = new Object2LongHashMap<>(MISSING_VALUE);

    @Test
    public void shouldInitiallyBeEmpty() {
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void getShouldReturnMissingValueWhenEmpty() {
        assertEquals(MISSING_VALUE, map.getValue("1"));
    }

    @Test
    public void getShouldReturnMissingValueWhenThereIsNoElement() {
        map.put("1", 1L);

        assertEquals(MISSING_VALUE, map.getValue("2"));
    }

    @Test
    public void getShouldReturnPutValues() {
        map.put("1", 1L);

        assertEquals(1L, map.getValue("1"));
    }

    @Test
    public void putShouldReturnOldValue() {
        map.put("1", 1L);

        assertEquals(1L, map.put("1", 2L));
    }

    @Test
    public void clearShouldResetSize() {
        map.put("1", 1L);
        map.put("100", 100L);

        map.clear();

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void clearShouldRemoveValues() {
        map.put("1", 1L);
        map.put("100", 100L);

        map.clear();

        assertEquals(MISSING_VALUE, map.getValue("1"));
        assertEquals(MISSING_VALUE, map.getValue("100"));
    }

    @Test
    public void shouldNotContainKeyOfAMissingKey() {
        assertFalse(map.containsKey("1"));
    }

    @Test
    public void shouldContainKeyOfAPresentKey() {
        map.put("1", 1L);

        assertTrue(map.containsKey("1"));
    }

    @Test
    public void shouldNotContainValueForAMissingEntry() {
        assertFalse(map.containsValue(1L));
    }

    @Test
    public void shouldContainValueForAPresentEntry() {
        map.put("1", 1L);

        assertTrue(map.containsValue(1L));
    }

    @Test
    public void shouldExposeValidKeySet() {
        map.put("1", 1L);
        map.put("2", 2L);

        assertKeysContainsElements(map.keySet());
    }

    @Test
    public void shouldExposeValidValueSet() {
        map.put("1", 1L);
        map.put("2", 2L);

        assertValuesContainsElements(map.values());
    }

    @Test
    public void shouldPutAllMembersOfAnotherHashMap() {
        map.put("1", 1L);
        map.put("2", 3L);

        final Map<String, Long> other = new HashMap<>();
        other.put("1", 2L);
        other.put("3", 4L);

        map.putAll(other);

        assertEquals(3, map.size());

        assertEquals(2, map.getValue("1"));
        assertEquals(3, map.getValue("2"));
        assertEquals(4, map.getValue("3"));
    }

    @Test
    public void entrySetShouldContainEntries() {
        map.put("1", 1L);
        map.put("2", 3L);

        final Set<Entry<String, Long>> entrySet = map.entrySet();
        assertEquals(2, entrySet.size());
        assertFalse(entrySet.isEmpty());

        final Iterator<Entry<String, Long>> it = entrySet.iterator();
        assertTrue(it.hasNext());
        assertEntryIs(it.next(), "2", 3L);
        assertTrue(it.hasNext());
        assertEntryIs(it.next(), "1", 1L);
        assertFalse(it.hasNext());
    }

    @Test
    public void removeShouldReturnMissing() {
        assertEquals(MISSING_VALUE, map.removeKey("1"));
    }

    @Test
    public void removeShouldReturnValueRemoved() {
        map.put("1", 2L);

        assertEquals(2L, map.removeKey("1"));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void removeShouldRemoveEntry() {
        map.put("1", 2L);

        map.remove("1");

        assertTrue(map.isEmpty());
        assertFalse(map.containsKey("1"));
        assertFalse(map.containsValue(2L));
    }

    @Test
    public void shouldOnlyRemoveTheSpecifiedEntry() {
        for (int i = 0; i < 8; i++) {
            map.put(Integer.toString(i), i * 2);
        }

        map.remove("5");

        for (int i = 0; i < 8; i++) {
            if (i != 5) {
                assertTrue(map.containsKey(Integer.toString(i)));
                assertTrue(map.containsValue(2 * i));
            }
        }
    }

    @Test
    public void shouldResizeWhenMoreElementsAreAdded() {
        for (int key = 0; key < 100; key++) {
            final int value = key * 2;
            assertEquals(MISSING_VALUE, map.put(Integer.toString(key), value));
            assertEquals(value, map.getValue(Integer.toString(key)));
        }
    }

    @Test
    public void toStringShouldReportAllEntries() {
        map.put("1", 2);
        map.put("3", 4);
        String string = map.toString();
        assertTrue(string.equals("{1=2, 3=4}") || string.equals("{3=4, 1=2}"));
    }

    private static void assertEntryIs(final Entry<String, Long> entry, final String expectedKey, final long expectedValue) {
        assertEquals(expectedKey, entry.getKey());
        assertEquals(expectedValue, entry.getValue().longValue());
    }

    private static void assertKeysContainsElements(final Collection<String> keys) {
        assertEquals(2, keys.size());
        assertFalse(keys.isEmpty());
        assertTrue(keys.contains("1"));
        assertTrue(keys.contains("2"));
        assertFalse(keys.contains("3"));
        assertThat(keys, hasItems("1", "2"));

        assertThat("iterator has failed to be reset", keys, hasItems("1", "2"));
    }

    private static void assertValuesContainsElements(final Collection<Long> values) {
        assertEquals(2, values.size());
        assertFalse(values.isEmpty());
        assertTrue(values.contains(1L));
        assertTrue(values.contains(2L));
        assertFalse(values.contains(3L));
        assertThat(values, hasItems(1L, 2L));

        assertThat("iterator has failed to be reset", values, hasItems(1L, 2L));
    }

    @Test
    public void sizeShouldReturnNumberOfEntries() {
        final int count = 100;
        for (int key = 0; key < count; key++) {
            map.put(Integer.toString(key), 1);
        }

        assertEquals(count, map.size());
    }

}
