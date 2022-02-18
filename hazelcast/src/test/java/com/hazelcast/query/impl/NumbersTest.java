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

package com.hazelcast.query.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.query.impl.Numbers.canonicalizeForHashLookup;
import static com.hazelcast.query.impl.Numbers.compare;
import static com.hazelcast.query.impl.Numbers.equal;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NumbersTest {

    @SuppressWarnings("ConstantConditions")
    @Test(expected = Throwable.class)
    public void testNullLhsInEqualThrows() {
        equal(null, 1);
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = Throwable.class)
    public void testNullRhsInEqualThrows() {
        equal(1, null);
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = Throwable.class)
    public void testNullLhsInCompareThrows() {
        compare(null, 1);
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = Throwable.class)
    public void testNullRhsInCompareThrows() {
        compare(1, null);
    }

    @Test(expected = Throwable.class)
    public void testNonNumbersTypesInCompare() {
        compare("string", 1);
    }

    @Test
    public void testEqual() {
        assertNotEqual(1L, 2);
        assertEqual(1, 1L);
        assertEqual(1, (short) 1);
        assertEqual(1, (byte) 1);

        assertNotEqual(new AtomicLong(1), new AtomicInteger(1));

        assertNotEqual(1, 1.1);
        // 1.100000000000000088817841970012523233890533447265625 != 1.10000002384185791015625
        assertNotEqual(1.1, 1.1F);
        assertEqual(1, 1.0);
        assertEqual(1, 1.0F);
        assertEqual(1.0F, 1.0);
        assertEqual(1.5F, 1.5);
        assertEqual(1.1F, (double) 1.1F);

        assertEqual(0, 0.0);
        assertNotEqual(0, -0.0);
        assertNotEqual(Long.MIN_VALUE, Double.NEGATIVE_INFINITY);
        assertNotEqual(Long.MAX_VALUE, Double.POSITIVE_INFINITY);
        assertNotEqual(0, Double.NaN);

        assertNotEqual(Long.MAX_VALUE, Long.MAX_VALUE + 5000.0);
        assertNotEqual(Long.MIN_VALUE, Long.MIN_VALUE - 5000.0);

        assertEqual(1L << 53, 0x1p53);
        // with Double, all things are possible
        assertEqual(1L << 53, 0x1p53 + 1);
        assertNotEqual(1L << 53, 0x1p53 - 1);
        assertEqual((1L << 53) - 1, 0x1p53 - 1);
        assertEqual((1L << 53) + 2, 0x1p53 + 2);

        assertEqual(-(1L << 53), -0x1p53);
        assertEqual(-(1L << 53), -0x1p53 - 1);
        assertNotEqual(-(1L << 53), -0x1p53 + 1);
        assertEqual(-(1L << 53) + 1, -0x1p53 + 1);
        assertEqual(-(1L << 53) - 2, -0x1p53 - 2);

        assertNotEqual(Integer.MAX_VALUE, Long.MAX_VALUE);
        assertNotEqual(Integer.MIN_VALUE, Long.MIN_VALUE);
    }

    @Test
    public void testCompare() {
        assertCompare(1L, 2, -1);
        assertCompare(1, 1L, 0);
        assertCompare(1, (short) 1, 0);
        assertCompare(1, (byte) 1, 0);

        assertCompare(1, 1.1, -1);
        // 1.100000000000000088817841970012523233890533447265625 < 1.10000002384185791015625
        assertCompare(1.1, 1.1F, -1);
        assertCompare(1, 1.0, 0);
        assertCompare(1, 1.0F, 0);
        assertCompare(1.0F, 1.0, 0);
        assertCompare(1.5F, 1.5, 0);
        assertCompare(1.1F, (double) 1.1F, 0);

        assertCompare(0, 0.0, 0);
        assertCompare(0, -0.0, +1);
        assertCompare(Long.MIN_VALUE, Double.NEGATIVE_INFINITY, +1);
        assertCompare(Long.MAX_VALUE, Double.POSITIVE_INFINITY, -1);
        assertCompare(0, Double.NaN, -1);
        assertCompare(Long.MAX_VALUE, Double.NaN, -1);

        assertCompare(Long.MAX_VALUE, Long.MAX_VALUE + 5000.0, -1);
        assertCompare(Long.MIN_VALUE, Long.MIN_VALUE - 5000.0, +1);

        assertCompare(1L << 53, 0x1p53, 0);
        // with Double, all things are possible
        assertCompare(1L << 53, 0x1p53 + 1, 0);
        assertCompare(1L << 53, 0x1p53 - 1, +1);
        assertCompare((1L << 53) - 1, 0x1p53 - 1, 0);
        assertCompare((1L << 53) + 2, 0x1p53 + 2, 0);

        assertCompare(-(1L << 53), -0x1p53, 0);
        assertCompare(-(1L << 53), -0x1p53 - 1, 0);
        assertCompare(-(1L << 53), -0x1p53 + 1, -1);
        assertCompare(-(1L << 53) + 1, -0x1p53 + 1, 0);
        assertCompare(-(1L << 53) - 2, -0x1p53 - 2, 0);

        assertCompare(Integer.MAX_VALUE, Long.MAX_VALUE, -1);
        assertCompare(Integer.MIN_VALUE, Long.MIN_VALUE, +1);
    }

    @Test
    public void testCanonicalization() {
        assertEqualCanonicalization(0.0, 0L, 0.0F, 0, (short) 0, (byte) 0);
        assertEqualCanonicalization(11.0, 11L, 11.0F, 11, (short) 11, (byte) 11);
        assertEqualCanonicalization(0.25, 0.25F);
        assertEqualCanonicalization(Double.NaN, Float.NaN);

        // 1.100000000000000088817841970012523233890533447265625 != 1.10000002384185791015625
        assertNotEqualCanonicalization(1.1, 1.1F);
        assertNotEqualCanonicalization(Math.nextUp(0.0), Math.nextUp(0.0F), 0);
        assertNotEqualCanonicalization(-0.0, 0);
        assertNotEqualCanonicalization(Double.NaN, 0);
    }

    private static void assertEqual(Number lhs, Number rhs) {
        assertTrue(equal(lhs, rhs));
        assertTrue(equal(rhs, lhs));
    }

    private static void assertNotEqual(Number lhs, Number rhs) {
        assertFalse(equal(lhs, rhs));
        assertFalse(equal(rhs, lhs));
    }

    private static void assertCompare(Comparable lhs, Comparable rhs, int expected) {
        if (expected == 0) {
            assertEquals(0, compare(lhs, rhs));
            assertEquals(0, compare(rhs, lhs));
        } else if (expected < 0) {
            assertThat(compare(lhs, rhs), lessThan(0));
            assertThat(compare(rhs, lhs), greaterThan(0));
        } else {
            assertThat(compare(lhs, rhs), greaterThan(0));
            assertThat(compare(rhs, lhs), lessThan(0));
        }
    }

    private static void assertEqualCanonicalization(Comparable... values) {
        Comparable expected = canonicalizeForHashLookup(values[0]);
        for (int i = 1; i < values.length; ++i) {
            Comparable actual = canonicalizeForHashLookup(values[i]);
            assertEquals(expected, actual);
            assertEquals(expected.hashCode(), actual.hashCode());
        }
    }

    private static void assertNotEqualCanonicalization(Comparable... values) {
        Comparable unexpected = canonicalizeForHashLookup(values[0]);
        for (int i = 1; i < values.length; ++i) {
            Comparable actual = canonicalizeForHashLookup(values[i]);
            assertNotEquals(unexpected, actual);
            // not checking for hash code values since they may be the same
        }
    }

}
