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

import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.hazelcast.query.impl.bitmap.SparseBitSet.ARRAY_STORAGE_16_MAX_SIZE;
import static com.hazelcast.query.impl.bitmap.SparseBitSet.ARRAY_STORAGE_32_MAX_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SparseBitSetTest {

    private final NavigableSet<Long> expected = new TreeSet<>();
    private final SparseBitSet actual = new SparseBitSet();

    @Test
    public void testAdd() {
        // try empty set
        verify();

        // at the beginning
        for (long i = 0; i < ARRAY_STORAGE_32_MAX_SIZE / 2; ++i) {
            set(i);
            verify();
            set(i);
            verify();
        }

        // offset
        for (long i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SIZE; ++i) {
            set(i);
            verify();
            set(i);
            verify();
        }

        // clear everything we have added
        for (long i = 0; i < ARRAY_STORAGE_32_MAX_SIZE / 2; ++i) {
            clear(i);
            verify();
        }
        for (long i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SIZE; ++i) {
            clear(i);
            verify();
        }

        // test empty again
        verify();

        // try gaps
        for (long i = 0; i < 1000; ++i) {
            set(i * i);
            verify();
            set(i * i);
            verify();
        }

        // try larger gaps
        for (long i = (long) Math.sqrt(Long.MAX_VALUE) - 1000; i < (long) Math.sqrt(Long.MAX_VALUE); ++i) {
            set(i * i);
            verify();
            set(i * i);
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
    public void testAddWithGapAndStorage32Upgrade() {
        for (long i = 555; i < 555 + ARRAY_STORAGE_32_MAX_SIZE + 1; ++i) {
            if (i != 560) {
                set(i);
            }
            verify();
        }
        set(560);
        verify();
    }

    @Test
    public void testAddWithStorage32Switching() {
        long prefix = ((long) Integer.MAX_VALUE * 2 + 1);

        for (long i = 100; i < 100 + ARRAY_STORAGE_32_MAX_SIZE + 10; ++i) {
            set(prefix + i);
            verify();
            set(prefix * 2 + i);
            verify();
        }
    }

    @Test
    public void testAddWithStorage16Upgrade() {
        for (long i = 555; i < 555 + ARRAY_STORAGE_16_MAX_SIZE + 10; ++i) {
            set(i);
            verify();
        }
    }

    @Test
    public void testAddWithStorage16UpgradeAndSwitching() {
        long prefix = ((long) Short.MAX_VALUE * 2 + 1);

        for (long i = 0; i < ARRAY_STORAGE_16_MAX_SIZE + 10; ++i) {
            set(i);
            verify();
            set(prefix + i);
            verify();
        }
    }

    @Test
    public void testRemove() {
        // try to clear empty set
        for (long i = 0; i < ARRAY_STORAGE_32_MAX_SIZE / 2; ++i) {
            clear(i);
            verify();
        }

        // at the beginning
        for (long i = 0; i < ARRAY_STORAGE_32_MAX_SIZE / 2; ++i) {
            set(i);
        }
        for (long i = 0; i < ARRAY_STORAGE_32_MAX_SIZE / 2 + 100; ++i) {
            clear(i);
            verify();
            // try nonexistent
            clear(i);
            verify();
        }

        // offset
        for (long i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SIZE; ++i) {
            set(i);
        }
        for (long i = 1000000 + ARRAY_STORAGE_32_MAX_SIZE + 100; i >= 1000000; --i) {
            clear(i);
            verify();
            // try nonexistent
            clear(i);
            verify();
        }

        // test empty again
        for (long i = 111; i < 1111; ++i) {
            clear(i);
            verify();
        }

        // try gaps
        for (long i = 0; i < 1000; ++i) {
            set(i * i);
        }
        for (long i = 0; i < 1000; ++i) {
            clear(i * i);
            verify();
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
    public void testRemoveWithStorage16Downgrade() {
        for (long i = 555; i < 555 + ARRAY_STORAGE_16_MAX_SIZE + 10; ++i) {
            set(i);
        }
        for (long i = 555; i < 555 + ARRAY_STORAGE_16_MAX_SIZE + 10; ++i) {
            clear(i);
            verify();
        }
    }

    @Test
    public void testIteratorAdvanceAtLeastTo() {
        // try empty set
        verifyAdvanceAtLeastTo();

        // at the beginning
        for (long i = 0; i < ARRAY_STORAGE_32_MAX_SIZE / 2; ++i) {
            set(i);
            verifyAdvanceAtLeastTo();
            set(i);
            verifyAdvanceAtLeastTo();
        }

        // offset
        for (long i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SIZE; ++i) {
            set(i);
            verifyAdvanceAtLeastTo();
            set(i);
            verifyAdvanceAtLeastTo();
        }

        // clear everything we have added
        for (long i = 0; i < ARRAY_STORAGE_32_MAX_SIZE / 2; ++i) {
            clear(i);
            verifyAdvanceAtLeastTo();
        }
        for (long i = 1000000; i < 1000000 + ARRAY_STORAGE_32_MAX_SIZE; ++i) {
            clear(i);
            verifyAdvanceAtLeastTo();
        }

        // test empty again
        verifyAdvanceAtLeastTo();

        // try gaps
        for (long i = 0; i < 1000; ++i) {
            set(i * i);
            verifyAdvanceAtLeastTo();
            set(i * i);
            verifyAdvanceAtLeastTo();
        }

        // try larger gaps
        for (long i = (long) Math.sqrt(Long.MAX_VALUE) - 1000; i < (long) Math.sqrt(Long.MAX_VALUE); ++i) {
            set(i * i);
            verifyAdvanceAtLeastTo();
            set(i * i);
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

        AscendingLongIterator iterator = actual.iterator();
        // try advance to the gap
        iterator.advanceAtLeastTo(prefix * 2 + 1);
        verify(iterator, expected.tailSet(prefix * 2 + 1));
    }

    @Test
    public void testPrefixIteration() {
        long prefix32 = ((long) Integer.MAX_VALUE * 2 + 1);
        long prefix16 = ((long) Short.MAX_VALUE * 2 + 1);

        // create a few top-level 32-bit prefix storages
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SIZE + 10; ++i) {
            set(i);
            verifyAdvanceAtLeastTo();
            set(prefix32 + i);
            verifyAdvanceAtLeastTo();
            set(2 * prefix32 + i);
            verifyAdvanceAtLeastTo();
            set(4 * prefix32 + i);
            verifyAdvanceAtLeastTo();

            verify();
        }

        // force creation of 16-bit array storages on lower level
        for (int i = 0; i < ARRAY_STORAGE_32_MAX_SIZE + 10; ++i) {
            set(i + prefix16);
            verifyAdvanceAtLeastTo();
            set(prefix32 + prefix16 + i);
            verifyAdvanceAtLeastTo();
            set(2 * prefix32 + prefix16 + i);
            verifyAdvanceAtLeastTo();
            set(4 * prefix32 + prefix16 + i);
            verifyAdvanceAtLeastTo();

            verify();
        }

        // force upgrade of 16-bit array storage to 16-bit bit storage
        for (int i = 0; i < ARRAY_STORAGE_16_MAX_SIZE + 10; ++i) {
            set(2 * prefix32 + prefix16 + i);
            verifyAdvanceAtLeastTo();
            verify();
        }
    }

    private void verify() {
        AscendingLongIterator iterator = actual.iterator();
        verify(iterator, expected);
    }

    private void verify(AscendingLongIterator actual, SortedSet<Long> expected) {
        long currentIndex = actual.getIndex();
        long current = actual.advance();
        for (Long expectedIndex : expected) {
            assertEquals(current, currentIndex);
            assertEquals((long) expectedIndex, current);
            currentIndex = actual.getIndex();
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
        verifyAdvanceAtLeastTo(actual.iterator(), Long.MAX_VALUE);
    }

    @SuppressWarnings("ConstantConditions")
    private void verifyAdvanceAtLeastTo(AscendingLongIterator actual, long step) {
        long previous = AscendingLongIterator.END;
        long current = actual.advanceAtLeastTo(step);
        long currentIndex = actual.getIndex();

        while (current != AscendingLongIterator.END) {
            long expectedIndex;
            if (previous == AscendingLongIterator.END) {
                expectedIndex = expected.ceiling(step);
            } else {
                expectedIndex = expected.ceiling(previous + step);
            }
            assertEquals(current, currentIndex);
            assertEquals(expectedIndex, currentIndex);

            previous = current;
            if (current <= Long.MAX_VALUE - step) {
                current = actual.advanceAtLeastTo(current + step);
                currentIndex = actual.getIndex();
            } else {
                break;
            }
        }

        assertEquals(current, currentIndex);
        if (previous == AscendingLongIterator.END) {
            assertNull(expected.ceiling(step));
        } else if (current == AscendingLongIterator.END) {
            assertNull(expected.ceiling(previous + step));
        } else {
            verify(actual, expected.tailSet(current));
        }
    }

    private void set(long index) {
        expected.add(index);
        actual.add(index);
    }

    private void clear(long index) {
        expected.remove(index);
        actual.remove(index);
    }

}
