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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SparseArrayTest {

    private final NavigableMap<Long, Long> expected = new TreeMap<>();
    private final SparseArray<Long> actual = new SparseArray<>();

    @Test
    public void testSet() {
        // try empty array
        verify();

        // at the beginning
        for (long i = 0; i < 1000; ++i) {
            set(i);
            verify();
            set(i, 100 + i);
            verify();
        }

        // offset
        for (long i = 1000000; i < 1000000 + 1000; ++i) {
            set(i);
            verify();
            set(i, 100 + i);
            verify();
        }

        // clear everything we have added
        for (long i = 0; i < 1000; ++i) {
            clear(i);
            verify();
        }
        for (long i = 1000000; i < 1000000 + 1000; ++i) {
            clear(i);
            verify();
        }

        // test empty again
        verify();

        // try gaps
        for (long i = 0; i < 1000; ++i) {
            set(i * i);
            verify();
            set(i * i, 100 + i * i);
            verify();
        }

        // try larger gaps
        for (long i = (long) Math.sqrt(Long.MAX_VALUE) - 1000; i < (long) Math.sqrt(Long.MAX_VALUE); ++i) {
            set(i * i);
            verify();
            set(i * i, 100 + i * i);
            verify();
        }

        // try some edge cases
        for (long i = 0; i <= 2; ++i) {
            set(i);
            verify();
        }
        for (long i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
            verify();
        }
        for (long i = Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set(i);
            verify();
        }
        for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 2; --i) {
            set(i);
            verify();
        }
    }

    @Test
    public void testClear() {
        // try to clear empty array
        actual.clear();
        verify();

        // at the beginning
        for (long i = 0; i < 1000; ++i) {
            set(i);
        }
        for (long i = 0; i < 1000 + 100; ++i) {
            clear(i);
            verify();
            // try nonexistent
            clear(i);
            verify();
        }

        // offset
        for (long i = 1000000; i < 1000000 + 1000; ++i) {
            set(i);
        }
        for (long i = 1000000 + 1000 + 100; i >= 1000000; --i) {
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
        for (long i = 0; i < 1000; ++i) {
            set(i * i);
        }
        for (long i = 0; i < 1000; ++i) {
            clear(i * i);
            verify();
        }

        // try larger gaps
        for (long i = (long) Math.sqrt(Long.MAX_VALUE) - 1000; i < (long) Math.sqrt(Long.MAX_VALUE); ++i) {
            set(i * i);
        }
        for (long i = (long) Math.sqrt(Long.MAX_VALUE) - 1000; i < (long) Math.sqrt(Long.MAX_VALUE); ++i) {
            clear(i * i);
            verify();
        }

        // try larger 2-element gaps
        for (long i = (long) Math.sqrt(Long.MAX_VALUE) - 1000; i < (long) Math.sqrt(Long.MAX_VALUE); ++i) {
            set(i * i);
            set(i * i - 1);
        }
        for (long i = (long) Math.sqrt(Long.MAX_VALUE) - 1000; i < (long) Math.sqrt(Long.MAX_VALUE); ++i) {
            clear(i * i);
            verify();
            clear(i * i - 1);
            verify();
        }

        // try some edge cases
        for (long i = 0; i <= 2; ++i) {
            set(i);
        }
        for (long i = 0; i <= 2; ++i) {
            clear(i);
            verify();
        }
        for (long i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
        }
        for (long i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            clear(i);
            verify();
        }
        for (long i = Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set(i);
        }
        for (long i = Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            clear(i);
            verify();
        }
        for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 2; --i) {
            set(i);
        }
        for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 2; --i) {
            clear(i);
            verify();
        }
    }

    @Test
    public void testIteratorAdvanceAtLeastTo() {
        // try empty array
        verifyAdvanceAtLeastTo();

        // at the beginning
        for (long i = 0; i < 1000; ++i) {
            set(i);
            verifyAdvanceAtLeastTo();
            set(i, 100 + i);
            verifyAdvanceAtLeastTo();
        }

        // offset
        for (long i = 1000000; i < 1000000 + 1000; ++i) {
            set(i);
            verifyAdvanceAtLeastTo();
            set(i, 100 + i);
            verifyAdvanceAtLeastTo();
        }

        // clear everything we have added
        for (long i = 0; i < 1000; ++i) {
            clear(i);
            verifyAdvanceAtLeastTo();
        }
        for (long i = 1000000; i < 1000000 + 1000; ++i) {
            clear(i);
            verifyAdvanceAtLeastTo();
        }

        // test empty again
        verifyAdvanceAtLeastTo();

        // try gaps
        for (long i = 0; i < 1000; ++i) {
            set(i * i);
            verifyAdvanceAtLeastTo();
            set(i * i, 100 + i * i);
            verifyAdvanceAtLeastTo();
        }

        // try larger gaps
        for (long i = (long) Math.sqrt(Long.MAX_VALUE) - 1000; i < (long) Math.sqrt(Long.MAX_VALUE); ++i) {
            set(i * i);
            verifyAdvanceAtLeastTo();
            set(i * i, 100 + i * i);
            verifyAdvanceAtLeastTo();
        }

        // try some edge cases
        for (long i = 0; i <= 2; ++i) {
            set(i);
            verifyAdvanceAtLeastTo();
        }
        for (long i = Short.MAX_VALUE - 2; i <= Short.MAX_VALUE + 2; ++i) {
            set(i);
            verifyAdvanceAtLeastTo();
        }
        for (long i = Integer.MAX_VALUE - 2; i <= (long) Integer.MAX_VALUE + 2; ++i) {
            set(i);
            verifyAdvanceAtLeastTo();
        }
        for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 2; --i) {
            set(i);
            verifyAdvanceAtLeastTo();
        }
    }

    @Test
    public void testIteratorAdvanceAtLeastToDistinctPrefixes() {
        long prefix = ((long) Integer.MAX_VALUE * 2 + 1);

        set(0);
        verifyAdvanceAtLeastTo();
        set(prefix + 1);
        verifyAdvanceAtLeastTo();
        set(prefix * 3 + 1);
        verifyAdvanceAtLeastTo();
        set(prefix * 4 + 1);
        verifyAdvanceAtLeastTo();
        set(prefix * 5 + 1);
        verifyAdvanceAtLeastTo();

        SparseArray.Iterator<Long> iterator = actual.iterator();
        // try advance to the gap
        iterator.advanceAtLeastTo(prefix * 2 + 1);
        verify(iterator, expected.tailMap(prefix * 2 + 1));
    }

    private void verify() {
        SparseArray.Iterator<Long> iterator = actual.iterator();
        verify(iterator, expected);
    }

    private void verify(SparseArray.Iterator<Long> actual, SortedMap<Long, Long> expected) {
        long currentIndex = actual.getIndex();
        Long currentValue = actual.getValue();
        long current = actual.advance();
        for (Map.Entry<Long, Long> expectedEntry : expected.entrySet()) {
            assertEquals(current, currentIndex);
            assertEquals((long) expectedEntry.getKey(), current);
            assertEquals(expectedEntry.getValue(), currentValue);
            currentIndex = actual.getIndex();
            currentValue = actual.getValue();
            current = actual.advance();
        }
        assertEquals(current, currentIndex);
        assertEquals(AscendingLongIterator.END, current);
    }

    private void verifyAdvanceAtLeastTo() {
        verifyAdvanceAtLeastTo(actual.iterator(), 1);
        verifyAdvanceAtLeastTo(actual.iterator(), 2);
        verifyAdvanceAtLeastTo(actual.iterator(), 5);
        verifyAdvanceAtLeastTo(actual.iterator(), Short.MAX_VALUE);
        verifyAdvanceAtLeastTo(actual.iterator(), Integer.MAX_VALUE);
        verifyAdvanceAtLeastTo(actual.iterator(), Long.MAX_VALUE / 2);
    }

    private void verifyAdvanceAtLeastTo(SparseArray.Iterator<Long> actual, long step) {
        long previous = SparseArray.Iterator.END;
        long current = actual.advanceAtLeastTo(step);
        long currentIndex = actual.getIndex();
        Long currentValue = actual.getValue();

        while (current != SparseArray.Iterator.END) {
            Map.Entry<Long, Long> expectedEntry;
            if (previous == SparseArray.Iterator.END) {
                expectedEntry = expected.ceilingEntry(step);
            } else {
                expectedEntry = expected.ceilingEntry(previous + step);
            }
            assertEquals(current, currentIndex);
            assertEquals((long) expectedEntry.getKey(), currentIndex);
            assertEquals((long) expectedEntry.getValue(), (long) currentValue);

            previous = current;
            if (current <= Long.MAX_VALUE - step) {
                current = actual.advanceAtLeastTo(current + step);
                currentIndex = actual.getIndex();
                currentValue = actual.getValue();
            } else {
                break;
            }
        }

        assertEquals(current, currentIndex);
        if (previous == SparseArray.Iterator.END) {
            assertNull(expected.ceilingEntry(step));
        } else if (current == SparseArray.Iterator.END) {
            assertNull(expected.ceilingEntry(previous + step));
        } else {
            verify(actual, expected.tailMap(current));
        }
    }

    private void set(long index) {
        expected.put(index, index + 1);
        actual.set(index, index + 1);
    }

    private void set(long index, long value) {
        expected.put(index, value);
        actual.set(index, value);
    }

    private void clear(long index) {
        expected.remove(index);
        actual.clear(index);
    }

}
