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

package com.hazelcast.query.impl.bitmap;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.hazelcast.query.impl.bitmap.BitmapUtils.capacityDeltaInt;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.denseCapacityDeltaShort;
import static com.hazelcast.query.impl.bitmap.SparseIntArray.ARRAY_STORAGE_32_MAX_SPARSE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SparseIntArrayTest {

    private final NavigableMap<Integer, Integer> expected = new TreeMap<>(Integer::compareUnsigned);
    private final SparseIntArray<Integer> actual = new SparseIntArray<>();

    @Test
    public void testSet() {
        // try empty array
        verify();

        // try dense
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i);
            verify();
            set(i, 100 + i);
            verify();
        }

        // go sparse
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            set(i);
            verify();
            set(i, 100 + i);
            verify();
        }

        // clear everything we have added
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            clear(i);
            verify();
        }
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            clear(i);
            verify();
        }

        // test empty again
        verify();

        // try gaps
        for (int i = 0; i < 1000; ++i) {
            set(i * i);
            verify();
            set(i * i, 100 + i * i);
            verify();
        }

        // try larger gaps
        for (int i = (int) Math.sqrt(Integer.MAX_VALUE) - 1000; i < (int) Math.sqrt(Integer.MAX_VALUE); ++i) {
            set(i * i);
            verify();
            set(i * i, 100 + i * i);
            verify();
        }

        // try some edge cases
        for (int i = -2; i <= 2; ++i) {
            set(i);
            verify();
        }
        for (int i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
            verify();
        }
        for (int i = Short.MIN_VALUE - 2; i <= Short.MIN_VALUE + 2; ++i) {
            set(i);
            verify();
        }
        for (long i = (long) Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set((int) i);
            verify();
        }
        for (long i = (long) Integer.MIN_VALUE - 2; i <= (long) Integer.MIN_VALUE + 2; ++i) {
            set((int) i);
            verify();
        }
    }

    @Test
    public void testSetDenseToSparse32WithMaxExceeded() {
        // add some dense entries
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            set(i);
            verify();
        }

        // go far beyond the last index to trigger dense to sparse conversion
        set(ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 1000);
        verify();

        // make sure we are still good
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 5; ++i) {
            set(i);
            verify();
        }
    }

    @Test
    public void testSetDenseToSparse32WithCapacityEqualToSize() {
        // add some dense entries
        for (int i = 0; i < capacityDeltaInt(0) + 1; ++i) {
            set(i);
            verify();
        }
        // at this point capacity is equal to size

        // go far beyond the last index to trigger dense to sparse conversion
        set(ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 1000);
        verify();

        // make sure we are still good
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 5; ++i) {
            set(i);
            verify();
        }
    }

    @Test
    public void testSetSparseToDense32() {
        // add some sparse entries
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i * 2);
            verify();
        }

        // restore density
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i * 2 + 1);
            verify();
        }

        // make sure we still can insert something
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 5; ++i) {
            set(i);
            verify();
        }
    }

    @Test
    public void testSetSparse16WithCapacityEqualToSize() {
        // add some dense entries
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            set(i);
            verify();
        }

        // go far beyond the last index to trigger dense to sparse conversion
        set(ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 1000);
        verify();

        // keep adding entries to trigger prefix storage creation
        int threshold = computeCapacityAfterInsertion(ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 2);
        for (int i = 0; i < threshold; ++i) {
            set(i);
            verify();
        }

        // trigger dense to sparse conversion in 16-bit storage
        set(ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 4);
        verify();

        // add more to the prefix eventually restoring the density
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 4; ++i) {
            set(i);
            verify();
        }
    }

    @Test
    public void testClear() {
        // try to clear empty array
        actual.clear();
        verify();

        // try dense
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i);
        }
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2 + 100; ++i) {
            clear(i);
            verify();
            // try nonexistent
            clear(i);
            verify();
        }

        // go sparse
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            set(i);
        }
        for (int i = 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE + 100; i >= 1000000; --i) {
            clear(i);
            verify();
            // try nonexistent
            clear(i);
            verify();
        }

        // test empty again
        actual.clear();
        verify();

        // try gaps
        for (int i = 0; i < 1000; ++i) {
            set(i * i);
        }
        for (int i = 0; i < 1000; ++i) {
            clear(i * i);
            verify();
        }

        // try larger gaps
        for (int i = (int) Math.sqrt(Integer.MAX_VALUE) - 1000; i < (int) Math.sqrt(Integer.MAX_VALUE); ++i) {
            set(i * i);
        }
        for (int i = (int) Math.sqrt(Integer.MAX_VALUE) - 1000; i < (int) Math.sqrt(Integer.MAX_VALUE); ++i) {
            clear(i * i);
            verify();
        }

        // try some edge cases
        for (int i = -2; i <= 2; ++i) {
            set(i);
        }
        for (int i = -2; i <= 2; ++i) {
            clear(i);
            verify();
        }
        for (int i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
        }
        for (int i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            clear(i);
            verify();
        }
        for (int i = Short.MIN_VALUE - 2; i <= Short.MIN_VALUE + 2; ++i) {
            set(i);
        }
        for (int i = Short.MIN_VALUE - 2; i <= Short.MIN_VALUE + 2; ++i) {
            clear(i);
            verify();
        }
        for (long i = (long) Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set((int) i);
        }
        for (long i = (long) Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            clear((int) i);
            verify();
        }
        for (long i = (long) Integer.MIN_VALUE - 2; i <= (long) Integer.MIN_VALUE + 2; ++i) {
            set((int) i);
        }
        for (long i = (long) Integer.MIN_VALUE - 2; i <= (long) Integer.MIN_VALUE + 2; ++i) {
            clear((int) i);
            verify();
        }
    }

    @Test
    public void testClearDenseCleanup() {
        // try dense
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i);
        }
        for (int i = ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2 + 100; i >= 0; --i) {
            clear(i);
            verify();
            // try nonexistent
            clear(i);
            verify();
        }
    }

    @Test
    public void testClearDenseCleanupWithGap() {
        // try dense
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 2; ++i) {
            set(i);
        }

        // make a gap
        clear(ARRAY_STORAGE_32_MAX_SPARSE_SIZE);
        verify();

        // continue removing
        for (int i = ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 2; i >= 0; --i) {
            clear(i);
            verify();
            // try nonexistent
            clear(i);
            verify();
            // try cleaning nonexistent with offset in the same prefix
            clear(ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 3 + i);
            verify();
            // try cleaning some really faraway entry
            clear(ARRAY_STORAGE_32_MAX_SPARSE_SIZE * 1000 + i);
            verify();
        }
    }

    @Test
    public void testGet() {
        // try empty array
        verify(0);
        verify(1000);
        verify(-1);
        verify();

        // try dense
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i);
            verify(i);
            verify();
            set(i, 100 + i);
            verify(i);
            verify();
        }

        // go sparse
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            set(i);
            verify(i);
            verify();
            set(i, 100 + i);
            verify(i);
            verify();
        }

        // clear everything we have added
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            clear(i);
            verify(i);
            verify();
        }
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            clear(i);
            verify(i);
            verify();
        }

        // test empty again
        verify(0);
        verify(1000);
        verify(-1);
        verify();

        // try gaps
        for (int i = 0; i < 1000; ++i) {
            set(i * i);
            verify(i * i);
            verify();
            set(i * i, 100 + i * i);
            verify(i * i);
            verify();
        }

        // try larger gaps
        for (int i = (int) Math.sqrt(Integer.MAX_VALUE) - 1000; i < (int) Math.sqrt(Integer.MAX_VALUE); ++i) {
            set(i * i);
            verify(i * i);
            verify();
            set(i * i, 100 + i * i);
            verify(i * i);
            verify();
        }

        // try some edge cases
        for (int i = -2; i <= 2; ++i) {
            set(i);
            verify(i);
            verify();
        }
        for (int i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
            verify(i);
            verify();
        }
        for (int i = Short.MIN_VALUE - 2; i <= Short.MIN_VALUE + 2; ++i) {
            set(i);
            verify(i);
            verify();
        }
        for (long i = (long) Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set((int) i);
            verify((int) i);
            verify();
        }
        for (long i = (long) Integer.MIN_VALUE - 2; i <= (long) Integer.MIN_VALUE + 2; ++i) {
            set((int) i);
            verify((int) i);
            verify();
        }
    }

    @Test
    public void testIterate() {
        SparseIntArray.Iterator<Integer> iterator = new SparseIntArray.Iterator<>();

        // test empty array
        verifyIterate(iterator);

        // try dense
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i);
            verifyIterate(iterator);
        }

        // go sparse
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            set(i);
            verifyIterate(iterator);
        }

        // clear everything we have added
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            clear(i);
            verifyIterate(iterator);
        }
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            clear(i);
            verifyIterate(iterator);
        }

        // test empty again
        verifyIterate(iterator);

        // try gaps
        for (int i = 0; i < 1000; ++i) {
            set(i * i);
            verifyIterate(iterator);
        }

        // try larger gaps
        for (int i = (int) Math.sqrt(Integer.MAX_VALUE) - 1000; i < (int) Math.sqrt(Integer.MAX_VALUE); ++i) {
            set(i * i);
            verifyIterate(iterator);
        }

        // try some edge cases
        for (int i = -2; i <= 2; ++i) {
            set(i);
            verifyIterate(iterator);
        }
        for (int i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
            verifyIterate(iterator);
        }
        for (int i = Short.MIN_VALUE - 2; i <= Short.MIN_VALUE + 2; ++i) {
            set(i);
            verifyIterate(iterator);
        }
        for (long i = (long) Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set((int) i);
            verifyIterate(iterator);
        }
        for (long i = (long) Integer.MIN_VALUE - 2; i <= (long) Integer.MIN_VALUE + 2; ++i) {
            set((int) i);
            verifyIterate(iterator);
        }
    }

    @Test
    public void testIterateAtLeastFrom() {
        SparseIntArray.Iterator<Integer> iterator = new SparseIntArray.Iterator<>();

        // test empty array
        verifyIterateAtLeastFrom(0, iterator);

        // try dense
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i);
            verifyIterateAtLeastFrom(i, iterator);
        }

        // go sparse
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            set(i);
            verifyIterateAtLeastFrom(i, iterator);
        }

        // clear everything we have added
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            clear(i);
            verifyIterateAtLeastFrom(i, iterator);
        }

        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            clear(i);
            verifyIterateAtLeastFrom(i, iterator);
        }

        // test empty again
        verifyIterateAtLeastFrom(100, iterator);

        // try gaps
        for (int i = 0; i < 1000; ++i) {
            set(i * i);
            verifyIterateAtLeastFrom(i, iterator);
        }

        // try larger gaps
        for (int i = (int) Math.sqrt(Integer.MAX_VALUE) - 1000; i < (int) Math.sqrt(Integer.MAX_VALUE); ++i) {
            set(i * i);
            verifyIterateAtLeastFrom(i, iterator);
        }

        // try some edge cases
        for (int i = -2; i <= 2; ++i) {
            set(i);
            verifyIterateAtLeastFrom(i, iterator);
        }
        for (int i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
            verifyIterateAtLeastFrom(i, iterator);
        }
        for (int i = Short.MIN_VALUE - 2; i <= Short.MIN_VALUE + 2; ++i) {
            set(i);
            verifyIterateAtLeastFrom(i, iterator);
        }
        for (long i = (long) Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set((int) i);
            verifyIterateAtLeastFrom((int) i, iterator);
        }
        for (long i = (long) Integer.MIN_VALUE - 2; i <= (long) Integer.MIN_VALUE + 2; ++i) {
            set((int) i);
            verifyIterateAtLeastFrom((int) i, iterator);
        }
    }

    @Test
    public void testAdvanceAtLeastTo() {
        SparseIntArray.Iterator<Integer> iterator = new SparseIntArray.Iterator<>();

        // test empty array
        verifyAdvanceAtLeastTo(iterator);

        // try dense
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            set(i);
            verifyAdvanceAtLeastTo(iterator);
        }

        // go sparse
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            set(i);
            verifyAdvanceAtLeastTo(iterator);
        }

        // clear everything we have added
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SPARSE_SIZE / 2; ++i) {
            clear(i);
            verifyAdvanceAtLeastTo(iterator);
        }
        for (int i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SPARSE_SIZE; ++i) {
            clear(i);
            verifyAdvanceAtLeastTo(iterator);
        }

        // test empty again
        verifyAdvanceAtLeastTo(iterator);

        // try gaps
        for (int i = 0; i < 1000; ++i) {
            set(i * i);
            verifyAdvanceAtLeastTo(iterator);
        }

        // try larger gaps
        for (int i = (int) Math.sqrt(Integer.MAX_VALUE) - 1000; i < (int) Math.sqrt(Integer.MAX_VALUE); ++i) {
            set(i * i);
            verifyAdvanceAtLeastTo(iterator);
        }

        // try some edge cases
        for (int i = -2; i <= 2; ++i) {
            set(i);
            verifyAdvanceAtLeastTo(iterator);
        }
        for (int i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
            verifyAdvanceAtLeastTo(iterator);
        }
        for (int i = Short.MIN_VALUE - 2; i <= Short.MIN_VALUE + 2; ++i) {
            set(i);
            verifyAdvanceAtLeastTo(iterator);
        }
        for (long i = (long) Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set((int) i);
            verifyAdvanceAtLeastTo(iterator);
        }
        for (long i = (long) Integer.MIN_VALUE - 2; i <= (long) Integer.MIN_VALUE + 2; ++i) {
            set((int) i);
            verifyAdvanceAtLeastTo(iterator);
        }
    }

    private void set(int index, int value) {
        expected.put(index, value);
        actual.set(index, value);
    }

    private void set(int index) {
        expected.put(index, index + 1);
        actual.set(index, index + 1);
    }

    private void clear(int index) {
        expected.remove(index);
        actual.clear(index);
    }

    private void verify(int index) {
        assertEquals(expected.get(index), actual.get(index));
    }

    private void verify() {
        SparseIntArray.Iterator<Integer> actualIterator = new SparseIntArray.Iterator<>();
        long actualCurrent = actual.iterate(actualIterator);
        verify(actualCurrent, actualIterator, expected);
    }

    private void verifyIterate(SparseIntArray.Iterator<Integer> actualIterator) {
        long actualCurrent = actual.iterate(actualIterator);
        verify(actualCurrent, actualIterator, expected);
    }

    private void verifyIterateAtLeastFrom(int from, SparseIntArray.Iterator<Integer> iterator) {
        long current = actual.iterateAtLeastFrom(from - 10, iterator);
        verify(current, iterator, expected.tailMap(from - 10));

        current = actual.iterateAtLeastFrom(from - 1, iterator);
        verify(current, iterator, expected.tailMap(from - 1));

        current = actual.iterateAtLeastFrom(from, iterator);
        verify(current, iterator, expected.tailMap(from));

        current = actual.iterateAtLeastFrom(from + 1, iterator);
        verify(current, iterator, expected.tailMap(from + 1));

        current = actual.iterateAtLeastFrom(from + 10, iterator);
        verify(current, iterator, expected.tailMap(from + 10));
    }

    private void verifyAdvanceAtLeastTo(SparseIntArray.Iterator<Integer> iterator) {
        verifyAdvanceAtLeastTo(iterator, 1);
        verifyAdvanceAtLeastTo(iterator, 2);
        verifyAdvanceAtLeastTo(iterator, 5);
        verifyAdvanceAtLeastTo(iterator, 1000);
    }

    private void verifyAdvanceAtLeastTo(SparseIntArray.Iterator<Integer> iterator, int step) {
        long previous = SparseIntArray.Iterator.END;
        long current = actual.iterate(iterator);

        while (current != SparseIntArray.Iterator.END) {
            Map.Entry<Integer, Integer> expectedEntry;
            if (previous == SparseIntArray.Iterator.END) {
                expectedEntry = expected.firstEntry();
            } else {
                expectedEntry = expected.ceilingEntry((int) (previous + step));
            }
            assertEquals((int) expectedEntry.getKey(), (int) current);
            assertEquals((int) expectedEntry.getValue(), (int) iterator.getValue());

            if (current <= 0xFFFFFFFFL - step) {
                previous = current;
                current = actual.advanceAtLeastTo((int) (current + step), (int) current, iterator);
            } else {
                break;
            }
        }

        if (previous == SparseIntArray.Iterator.END) {
            assertTrue(expected.isEmpty());
        } else if (current == SparseIntArray.Iterator.END) {
            assertNull(expected.ceilingEntry((int) (previous + step)));
        } else {
            verify(current, iterator, expected.tailMap((int) current));
        }
    }

    private void verify(long actualCurrent, SparseIntArray.Iterator<Integer> actualIterator,
                        SortedMap<Integer, Integer> expected) {
        long current = actualCurrent;
        for (Map.Entry<Integer, Integer> expectedEntry : expected.entrySet()) {
            assertEquals((int) expectedEntry.getKey(), (int) current);
            assertEquals(expectedEntry.getValue(), actualIterator.getValue());
            current = actual.advance((int) current, actualIterator);
        }
        assertEquals(SparseIntArray.Iterator.END, current);
    }

    @SuppressWarnings("SameParameterValue")
    private static int computeCapacityAfterInsertion(int count) {
        int size = 0;
        int capacity = capacityDeltaInt(0);
        while (size < count) {
            if (capacity <= size) {
                capacity += denseCapacityDeltaShort(size, capacity);
            }
            ++size;
        }
        return capacity;
    }

}
