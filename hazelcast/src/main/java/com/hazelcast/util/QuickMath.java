/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

/**
 * The class {@code QuickMath} contains methods to perform optimized mathematical operations.
 * Methods are allowed to put additional constraints on the range of input values if required for efficiency.
 * Methods are <b>not</b> required to perform validation of input arguments, but they have to indicate the constraints
 * in theirs contract.
 */
public final class QuickMath {

    private QuickMath() {
    }

    /**
     * Return true if input argument is power of two.
     * Input has to be a a positive integer.
     * <p/>
     * Result is undefined for zero or negative integers.
     *
     * @param x
     * @return <code>true</code> if <code>x</code> is power of two
     */
    public static boolean isPowerOfTwo(long x) {
        return (x & (x - 1)) == 0;
    }

    /**
     * Computes the remainder of the division of <code>a</code> by <code>b</code>.
     * <code>a</code> has to be non-negative integer and <code>b</code> has to be power of two
     * otherwise the result is undefined.
     *
     * @param a
     * @param b
     * @return remainder of the division of a by b.
     */
    public static int modPowerOfTwo(int a, int b) {
        return a & (b - 1);
    }

    /**
     * Computes the remainder of the division of <code>a</code> by <code>b</code>.
     * <code>a</code> has to be non-negative integer and <code>b</code> has to be power of two
     * otherwise the result is undefined.
     *
     * @param a
     * @param b
     * @return remainder of the division of a by b.
     */
    public static long modPowerOfTwo(long a, int b) {
        return a & (b - 1);
    }

    public static int nextPowerOfTwo(int value) {
        if (!isPowerOfTwo(value)) {
            value--;
            value |= value >> 1;
            value |= value >> 2;
            value |= value >> 4;
            value |= value >> 8;
            value |= value >> 16;
            value++;
        }
        return value;
    }

    public static long nextPowerOfTwo(long value) {
        if (!isPowerOfTwo(value)) {
            value--;
            value |= value >> 1;
            value |= value >> 2;
            value |= value >> 4;
            value |= value >> 8;
            value |= value >> 16;
            value |= value >> 32;
            value++;
        }
        return value;
    }

    public static int log2(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    public static int log2(long value) {
        return 63 - Long.numberOfLeadingZeros(value);
    }

    public static int divideByAndCeilToInt(double d, int k) {
        return (int) Math.ceil(d / k);
    }

    public static long divideByAndCeilToLong(double d, int k) {
        return (long) Math.ceil(d / k);
    }

    public static int divideByAndRoundToInt(double d, int k) {
        return (int) Math.rint(d / k);
    }

    public static long divideByAndRoundToLong(double d, int k) {
        return (long) Math.rint(d / k);
    }

    public static int normalize(int value, int factor) {
        return divideByAndCeilToInt(value, factor) * factor;
    }

    public static long normalize(long value, int factor) {
        return divideByAndCeilToLong(value, factor) * factor;
    }

    /**
     * Compares two integers
     *
     * @param i1 First number to compare with second one
     * @param i2 Second number to compare with first one
     * @return +1 if i1 > i2, -1 if i2 > i1, 0 if i1 and i2 are equals
     */
    public static int compareIntegers(int i1, int i2) {
        if (i1 > i2) {
            return +1;
        } else if (i2 > i1) {
            return -1;
        } else {
            return 0;
        }
    }

    /**
     * Compares two longs
     *
     * @param l1 First number to compare with second one
     * @param l2 Second number to compare with first one
     * @return +1 if l1 > l2, -1 if l2 > l1, 0 if l1 and l2 are equals
     */
    public static int compareLongs(long l1, long l2) {
        if (l1 > l2) {
            return +1;
        } else if (l2 > l1) {
            return -1;
        } else {
            return 0;
        }
    }
}
