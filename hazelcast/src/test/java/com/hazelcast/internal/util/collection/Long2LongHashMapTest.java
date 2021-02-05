/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.collection.Long2LongHashMap.LongLongCursor;
import com.hazelcast.internal.util.function.LongLongConsumer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Long2LongHashMapTest {
    public static final long MISSING_VALUE = -1L;

    private Long2LongHashMap map = new Long2LongHashMap(MISSING_VALUE);

    @Test
    public void shouldInitiallyBeEmpty() {
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void getShouldReturnMissingValueWhenEmpty() {
        assertEquals(MISSING_VALUE, map.get(1L));
    }

    @Test
    public void getShouldReturnMissingValueWhenThereIsNoElement() {
        map.put(1L, 1L);

        assertEquals(MISSING_VALUE, map.get(2L));
    }

    @Test
    public void getShouldReturnPutValues() {
        map.put(1L, 1L);

        assertEquals(1L, map.get(1L));
    }

    @Test
    public void putShouldReturnOldValue() {
        map.put(1L, 1L);

        assertEquals(1L, map.put(1L, 2L));
    }

    @Test
    public void clearShouldResetSize() {
        map.put(1L, 1L);
        map.put(100L, 100L);

        map.clear();

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void clearShouldRemoveValues() {
        map.put(1L, 1L);
        map.put(100L, 100L);

        map.clear();

        assertEquals(MISSING_VALUE, map.get(1L));
        assertEquals(MISSING_VALUE, map.get(100L));
    }

    @Test
    public void forEachShouldLoopOverEveryElement() {
        map.put(1L, 1L);
        map.put(100L, 100L);

        final LongLongConsumer mockConsumer = mock(LongLongConsumer.class);
        map.longForEach(mockConsumer);

        final InOrder inOrder = inOrder(mockConsumer);
        inOrder.verify(mockConsumer).accept(1L, 1L);
        inOrder.verify(mockConsumer).accept(100L, 100L);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void cursorShouldLoopOverEveryElement() {
        map.put(1L, 1L);
        map.put(100L, 100L);
        final LongLongCursor cursor = map.cursor();
        assertTrue(cursor.advance());
        assertEquals(1L, cursor.key());
        assertEquals(1L, cursor.value());
        assertTrue(cursor.advance());
        assertEquals(100L, cursor.key());
        assertEquals(100L, cursor.value());
    }

    @Test
    public void shouldNotContainKeyOfAMissingKey() {
        assertFalse(map.containsKey(1L));
    }

    @Test
    public void shouldContainKeyOfAPresentKey() {
        map.put(1L, 1L);

        assertTrue(map.containsKey(1L));
    }

    @Test
    public void shouldNotContainValueForAMissingEntry() {
        assertFalse(map.containsValue(1L));
    }

    @Test
    public void shouldContainValueForAPresentEntry() {
        map.put(1L, 1L);

        assertTrue(map.containsValue(1L));
    }

    @Test
    public void shouldExposeValidKeySet() {
        map.put(1L, 1L);
        map.put(2L, 2L);

        assertCollectionContainsElements(map.keySet());
    }

    @Test
    public void shouldExposeValidValueSet() {
        map.put(1L, 1L);
        map.put(2L, 2L);

        assertCollectionContainsElements(map.values());
    }

    @Test
    public void shouldPutAllMembersOfAnotherHashMap() {
        map.put(1L, 1L);
        map.put(2L, 3L);

        final Map<Long, Long> other = new HashMap<Long, Long>();
        other.put(1L, 2L);
        other.put(3L, 4L);

        map.putAll(other);

        assertEquals(3, map.size());

        assertEquals(2, map.get(1L));
        assertEquals(3, map.get(2L));
        assertEquals(4, map.get(3L));
    }

    @Test
    public void entrySetShouldContainEntries() {
        map.put(1L, 1L);
        map.put(2L, 3L);

        final Set<Entry<Long, Long>> entrySet = map.entrySet();
        assertEquals(2, entrySet.size());
        assertFalse(entrySet.isEmpty());

        final Iterator<Entry<Long, Long>> it = entrySet.iterator();
        assertTrue(it.hasNext());
        assertEntryIs(it.next(), 2L, 3L);
        assertTrue(it.hasNext());
        assertEntryIs(it.next(), 1L, 1L);
        assertFalse(it.hasNext());
    }

    @Test
    public void removeShouldReturnMissing() {
        assertEquals(MISSING_VALUE, map.remove(1L));
    }

    @Test
    public void removeShouldReturnValueRemoved() {
        map.put(1L, 2L);

        assertEquals(2L, map.remove(1L));
    }

    @Test
    public void removeShouldRemoveEntry() {
        map.put(1L, 2L);

        map.remove(1L);

        assertTrue(map.isEmpty());
        assertFalse(map.containsKey(1L));
        assertFalse(map.containsValue(2L));
    }

    @Test
    public void shouldOnlyRemoveTheSpecifiedEntry() {
        for (int i = 0; i < 8; i++) {
            map.put(i, i * 2);
        }

        map.remove(5L);

        for (int i = 0; i < 8; i++) {
            if (i != 5) {
                assertTrue(map.containsKey(i));
                assertTrue(map.containsValue(2 * i));
            }
        }
    }

    @Test
    public void shouldResizeWhenMoreElementsAreAdded() {
        for (int key = 0; key < 100; key++) {
            final int value = key * 2;
            assertEquals(MISSING_VALUE, map.put(key, value));
            assertEquals(value, map.get(key));
        }
    }

    @Test
    public void toStringShouldReportAllEntries() {
        map.put(1, 2);
        map.put(3, 4);
        assertEquals("{1->2 3->4}", map.toString());
    }

    private static void assertEntryIs(final Entry<Long, Long> entry, final long expectedKey, final long expectedValue) {
        assertEquals(expectedKey, entry.getKey().longValue());
        assertEquals(expectedValue, entry.getValue().longValue());
    }

    private static void assertCollectionContainsElements(final Collection<Long> keys) {
        assertEquals(2, keys.size());
        assertFalse(keys.isEmpty());
        assertTrue(keys.contains(1L));
        assertTrue(keys.contains(2L));
        assertFalse(keys.contains(3L));
        assertThat(keys, hasItems(1L, 2L));

        assertThat("iterator has failed to be reset", keys, hasItems(1L, 2L));
    }

    @Test
    public void sizeShouldReturnNumberOfEntries() {
        final int count = 100;
        for (int key = 0; key < count; key++) {
            map.put(key, 1);
        }

        assertEquals(count, map.size());
    }
}
