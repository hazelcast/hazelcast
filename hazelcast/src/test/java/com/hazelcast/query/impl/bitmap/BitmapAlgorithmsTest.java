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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BitmapAlgorithmsTest {

    private final List<SparseBitSet> actual = new ArrayList<>();
    private final List<TreeSet<Long>> expected = new ArrayList<>();

    private final SparseArray<Long> actualUniverse = new SparseArray<>();
    private final TreeSet<Long> expectedUniverse = new TreeSet<>();

    @Test
    public void testAnd() {
        long seed = System.nanoTime();
        System.out.println(getClass().getSimpleName() + ".testAnd seed: " + seed);

        actual.add(new SparseBitSet());
        expected.add(new TreeSet<>());
        verifyAnd();
        actual.clear();
        expected.clear();

        generate(0, 100, 1);
        verifyAnd();

        generate(100, 100, 1);
        verifyAnd();

        generate(0, 75000, 1);
        verifyAnd();

        generate(100, 40000, 2);
        verifyAnd();

        generate(200, 30000, 3);
        verifyAnd();

        actual.add(new SparseBitSet());
        expected.add(new TreeSet<>());
        verifyAnd();
        actual.remove(actual.size() - 1);
        expected.remove(expected.size() - 1);

        generate(2000000, 30000, 3);
        verifyAnd();
        actual.remove(actual.size() - 1);
        expected.remove(expected.size() - 1);

        generateRandom(seed, 10000, 50000);
        verifyAnd();

        generateRandom(seed, 500000, -1);
        verifyAnd();
    }

    @Test
    public void testOr() {
        long seed = System.nanoTime();
        System.out.println(getClass().getSimpleName() + ".testOr seed: " + seed);

        actual.add(new SparseBitSet());
        expected.add(new TreeSet<>());
        verifyOr();
        actual.clear();
        expected.clear();

        generate(0, 75000, 1);
        verifyOr();

        generate(100, 40000, 2);
        verifyOr();

        generate(200, 30000, 3);
        verifyOr();

        actual.add(new SparseBitSet());
        expected.add(new TreeSet<>());
        verifyOr();
        actual.remove(actual.size() - 1);
        expected.remove(expected.size() - 1);

        generate(2000000, 30000, 3);
        verifyOr();
        actual.remove(actual.size() - 1);
        expected.remove(expected.size() - 1);

        generateRandom(seed, 10000, 50000);
        verifyOr();

        generateRandom(seed, 20000, -1);
        verifyOr();

        actual.clear();
        expected.clear();
        actual.add(new SparseBitSet());
        expected.add(new TreeSet<>());
        verifyOr();

        generateRandom(seed, 40000, 100000);
        verifyOr();

        generateRandom(seed, 40000, 100000);
        verifyOr();

        generateRandom(seed, 500000, -1);
        verifyOr();
    }

    @Test
    public void testNot() {
        long seed = System.nanoTime();
        System.out.println(getClass().getSimpleName() + ".testNot seed: " + seed);

        actual.add(new SparseBitSet());
        expected.add(new TreeSet<>());
        verifyNotAndThenClear();

        generateUniverse(0, 75000, 1);
        actual.add(new SparseBitSet());
        expected.add(new TreeSet<>());
        verifyNotAndThenClear();

        generateUniverse(0, 70000, 1);
        generate(0, 70000, 1);
        verifyNotAndThenClear();

        generateUniverse(0, 70000, 2);
        generate(0, 70000, 1);
        verifyNotAndThenClear();

        generateUniverse(100, 75000, 1);
        generateRandom(seed, 50000, 100000);
        verifyNotAndThenClear();

        generateUniverse(100, 75000, 2);
        generateRandom(seed, 50000, 100000);
        verifyNotAndThenClear();

        generateRandomUniverse(seed, 60000, 100000);
        generateRandom(seed, 60000, 100000);
        verifyNotAndThenClear();

        generateRandomUniverse(seed, 40000, 100000);
        generateRandom(seed, 40000, -1);
        verifyNotAndThenClear();

        generateRandomUniverse(seed, 75000, -1);
        generateRandom(seed, 75000, -1);
        verifyNotAndThenClear();

        generateRandomUniverse(seed, 1000, -1);
        generateRandom(seed, 1000, -1);
        actual.get(0).add(Long.MAX_VALUE);
        expected.get(0).add(Long.MAX_VALUE);
        verifyNotAndThenClear();

        generateRandomUniverse(seed, 2000, -1);
        generateRandom(seed, 2000, -1);
        actual.get(0).add(Long.MAX_VALUE - 1);
        actual.get(0).add(Long.MAX_VALUE);
        expected.get(0).add(Long.MAX_VALUE - 1);
        expected.get(0).add(Long.MAX_VALUE);
        verifyNotAndThenClear();

        generateRandomUniverse(seed, 500000, -1);
        generateRandom(seed, 500000, -1);
        actual.get(0).add(Long.MAX_VALUE - 2);
        actual.get(0).add(Long.MAX_VALUE - 1);
        actual.get(0).add(Long.MAX_VALUE);
        expected.get(0).add(Long.MAX_VALUE - 2);
        expected.get(0).add(Long.MAX_VALUE - 1);
        expected.get(0).add(Long.MAX_VALUE);
        verifyNotAndThenClear();
    }

    private void verifyAnd() {
        assert !actual.isEmpty();
        assert !expected.isEmpty();

        TreeSet<Long> expectedResult = null;
        for (TreeSet<Long> expectedSet : expected) {
            if (expectedResult == null) {
                expectedResult = new TreeSet<>(expectedSet);
            } else {
                expectedResult.retainAll(expectedSet);
            }
        }

        verify(BitmapAlgorithms.and(actualIterators()), expectedResult);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.and(actualIterators()), expectedResult, 1);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.and(actualIterators()), expectedResult, 2);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.and(actualIterators()), expectedResult, 5);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.and(actualIterators()), expectedResult, Short.MAX_VALUE);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.and(actualIterators()), expectedResult, Integer.MAX_VALUE);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.and(actualIterators()), expectedResult, Long.MAX_VALUE / 2);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.and(actualIterators()), expectedResult, Long.MAX_VALUE);
    }

    private void verifyOr() {
        assert !actual.isEmpty();
        assert !expected.isEmpty();

        TreeSet<Long> expectedResult = null;
        for (TreeSet<Long> expectedSet : expected) {
            if (expectedResult == null) {
                expectedResult = new TreeSet<>(expectedSet);
            } else {
                expectedResult.addAll(expectedSet);
            }
        }

        verify(BitmapAlgorithms.or(actualIterators()), expectedResult);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.or(actualIterators()), expectedResult, 1);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.or(actualIterators()), expectedResult, 2);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.or(actualIterators()), expectedResult, 5);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.or(actualIterators()), expectedResult, Short.MAX_VALUE);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.or(actualIterators()), expectedResult, Integer.MAX_VALUE);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.or(actualIterators()), expectedResult, Long.MAX_VALUE / 2);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.or(actualIterators()), expectedResult, Long.MAX_VALUE);
    }

    private void verifyNotAndThenClear() {
        assert actual.size() == 1;
        assert expected.size() == 1;

        SparseBitSet actual = this.actual.get(0);
        TreeSet<Long> expected = this.expected.get(0);

        AscendingLongIterator actualIterator = actual.iterator();
        for (long i = actualIterator.advance(); i != AscendingLongIterator.END; i = actualIterator.advance()) {
            actualUniverse.set(i, i);
        }
        expectedUniverse.addAll(expected);

        TreeSet<Long> expectedResult = new TreeSet<>(expectedUniverse);
        expectedResult.removeAll(expected);

        verify(BitmapAlgorithms.not(actual.iterator(), actualUniverse), expectedResult);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.not(actual.iterator(), actualUniverse), expectedResult, 1);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.not(actual.iterator(), actualUniverse), expectedResult, 2);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.not(actual.iterator(), actualUniverse), expectedResult, 5);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.not(actual.iterator(), actualUniverse), expectedResult, Short.MAX_VALUE);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.not(actual.iterator(), actualUniverse), expectedResult, Integer.MAX_VALUE);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.not(actual.iterator(), actualUniverse), expectedResult, Long.MAX_VALUE / 2);
        verifyAdvanceAtLeastTo(BitmapAlgorithms.not(actual.iterator(), actualUniverse), expectedResult, Long.MAX_VALUE);

        this.actual.clear();
        this.expected.clear();
        actualUniverse.clear();
        expectedUniverse.clear();
    }

    private AscendingLongIterator[] actualIterators() {
        AscendingLongIterator[] actualIterators = new AscendingLongIterator[actual.size()];
        for (int i = 0; i < actual.size(); ++i) {
            actualIterators[i] = actual.get(i).iterator();
        }
        return actualIterators;
    }

    private void generate(long offset, long count, long step) {
        SparseBitSet actual = new SparseBitSet();
        TreeSet<Long> expected = new TreeSet<>();
        for (long i = 0; i < count; ++i) {
            long index = offset + i * step;
            actual.add(index);
            expected.add(index);
        }

        verify(actual.iterator(), expected);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, 1);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, 2);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, 5);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, Short.MAX_VALUE);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, Integer.MAX_VALUE);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, Long.MAX_VALUE / 2);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, Long.MAX_VALUE);

        this.actual.add(actual);
        this.expected.add(expected);
    }

    private void generateRandom(long seed, int count, long range) {
        Random random = new Random(seed);
        if (range == -1) {
            range = random.nextLong() & Long.MAX_VALUE;
        }

        SparseBitSet actual = new SparseBitSet();
        TreeSet<Long> expected = new TreeSet<>();
        if (range != 0) {
            for (int i = 0; i < count; ++i) {
                long member = (random.nextLong() & Long.MAX_VALUE) % range;
                actual.add(member);
                expected.add(member);
            }
        }

        verify(actual.iterator(), expected);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, 1);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, 2);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, 5);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, Short.MAX_VALUE);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, Integer.MAX_VALUE);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, Long.MAX_VALUE / 2);
        verifyAdvanceAtLeastTo(actual.iterator(), expected, Long.MAX_VALUE);

        this.actual.add(actual);
        this.expected.add(expected);
    }

    private void generateUniverse(long offset, long count, long step) {
        for (long i = 0; i < count; ++i) {
            long index = offset + i * step;
            actualUniverse.set(index, index);
            expectedUniverse.add(index);
        }

        verify(actualUniverse.iterator(), expectedUniverse);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, 1);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, 2);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, 5);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, Short.MAX_VALUE);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, Integer.MAX_VALUE);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, Long.MAX_VALUE / 2);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, Long.MAX_VALUE);
    }

    private void generateRandomUniverse(long seed, int count, long range) {
        Random random = new Random(seed);
        if (range == -1) {
            range = random.nextLong() & Long.MAX_VALUE;
        }

        if (range != 0) {
            for (int i = 0; i < count; ++i) {
                long member = (random.nextLong() & Long.MAX_VALUE) % range;
                actualUniverse.set(member, member);
                expectedUniverse.add(member);
            }
        }

        verify(actualUniverse.iterator(), expectedUniverse);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, 1);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, 2);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, 5);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, Short.MAX_VALUE);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, Integer.MAX_VALUE);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, Long.MAX_VALUE / 2);
        verifyAdvanceAtLeastTo(actualUniverse.iterator(), expectedUniverse, Long.MAX_VALUE);
    }

    private void verify(AscendingLongIterator actual, SortedSet<Long> expected) {
        long actualIndex = actual.getIndex();
        long actualCurrent = actual.advance();
        for (Long expectedIndex : expected) {
            assertEquals(actualCurrent, actualIndex);
            assertEquals((long) expectedIndex, actualCurrent);
            actualIndex = actual.getIndex();
            actualCurrent = actual.advance();
        }
        assertEquals(actualCurrent, actualIndex);
        assertEquals(AscendingLongIterator.END, actualCurrent);
    }

    @SuppressWarnings("ConstantConditions")
    private void verifyAdvanceAtLeastTo(AscendingLongIterator actual, TreeSet<Long> expected, long step) {
        long actualPrevious = AscendingLongIterator.END;
        long actualCurrent = actual.advanceAtLeastTo(step);
        long actualIndex = actual.getIndex();

        while (actualCurrent != AscendingLongIterator.END) {
            long expectedIndex;
            if (actualPrevious == AscendingLongIterator.END) {
                expectedIndex = expected.ceiling(step);
            } else {
                expectedIndex = expected.ceiling(actualPrevious + step);
            }
            assertEquals(actualCurrent, actualIndex);
            assertEquals(expectedIndex, actualIndex);

            actualPrevious = actualCurrent;
            if (actualCurrent <= Long.MAX_VALUE - step) {
                actualCurrent = actual.advanceAtLeastTo(actualCurrent + step);
                actualIndex = actual.getIndex();
            } else {
                break;
            }
        }

        assertEquals(actualCurrent, actualIndex);
        if (actualPrevious == AscendingLongIterator.END) {
            assertNull(expected.ceiling(step));
        } else if (actualCurrent == AscendingLongIterator.END) {
            assertNull(expected.ceiling(actualPrevious + step));
        } else {
            verify(actual, expected.tailSet(actualCurrent));
        }
    }

}
