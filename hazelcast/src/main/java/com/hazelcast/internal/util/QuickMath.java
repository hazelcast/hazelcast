/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 * Portions Copyright 2014 Real Logic Ltd.
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

/**
 * The class {@code QuickMath} contains methods to perform optimized mathematical operations.
 * Methods are allowed to put additional constraints on the range of input values if required for efficiency.
 * Methods are <b>not</b> required to perform validation of input arguments, but they have to indicate the constraints
 * in theirs contract.
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class QuickMath {

    private QuickMath() {
    }

    /**
     * Return true if input argument is power of two.
     * Input has to be a a positive integer.
     * <p>
     * Result is undefined for zero or negative integers.
     *
     * @param x test <code>x</code> to see if it is a power of two
     * @return <code>true</code> if <code>x</code> is power of two
     */
    public static boolean isPowerOfTwo(long x) {
        return (x & (x - 1)) == 0;
    }

    /**
     * Computes the remainder of the division of <code>a</code> by <code>b</code>.
     * <code>a</code> has to be a non-negative integer and <code>b</code> has to be a power of two,
     * otherwise the result is undefined.
     *
     * @param a divide a by b. a must be a non-negative integer
     * @param b divide a by b. b must be a power of two
     * @return remainder of the division of a by b.
     */
    public static int modPowerOfTwo(int a, int b) {
        return a & (b - 1);
    }

    /**
     * Computes the remainder of the division of <code>a</code> by <code>b</code>.
     * <code>a</code> has to be a non-negative integer and <code>b</code> has to be a power of two,
     * otherwise the result is undefined.
     *
     * @param a divide a by b. a must be a non-negative integer
     * @param b divide a by b. b must be a power of two
     * @return remainder of the division of a by b.
     */
    public static long modPowerOfTwo(long a, int b) {
        return a & (b - 1);
    }

    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value.
     * <p>
     * If the value is &lt;= 0 then 1 will be returned.
     * <p>
     * This method is not suitable for {@link Integer#MIN_VALUE} or numbers greater than 2^30.
     *
     * @param value from which to search for next power of 2
     * @return The next power of 2 or the value itself if it is a power of 2
     */
    public static int nextPowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value.
     * <p>
     * If the value is &lt;= 0 then 1 will be returned.
     * <p>
     * This method is not suitable for {@link Long#MIN_VALUE} or numbers greater than 2^62.
     *
     * @param value from which to search for next power of 2
     * @return The next power of 2 or the value itself if it is a power of 2
     */
    public static long nextPowerOfTwo(final long value) {
        return 1L << (64 - Long.numberOfLeadingZeros(value - 1));
    }

    /**
     * Return the log 2 result for this int.
     *
     * @param value the int value
     * @return the log 2 result for value
     */
    public static int log2(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    /**
     * Return the log 2 result for this long.
     *
     * @param value the long value
     * @return the log 2 result for value
     */
    public static int log2(long value) {
        return 63 - Long.numberOfLeadingZeros(value);
    }

    /**
     * Divide d by k and return the smallest integer greater than or equal to the result.
     *
     * @param d divide d by k
     * @param k divide d by k
     * @return the smallest integer greater than or equal to the result
     */
    public static int divideByAndCeilToInt(double d, int k) {
        return (int) Math.ceil(d / k);
    }

    /**
     * Divide d by k and return the smallest integer greater than or equal to the result.
     *
     * @param d divide d by k
     * @param k divide d by k
     * @return the smallest integer greater than or equal to the result
     */
    public static long divideByAndCeilToLong(double d, int k) {
        return (long) Math.ceil(d / k);
    }

    /**
     * Divide d by k and return the int value closest to the result.
     *
     * @param d divide d by k
     * @param k divide d by k
     * @return the int value closest to the result
     */
    public static int divideByAndRoundToInt(double d, int k) {
        return (int) Math.rint(d / k);
    }

    /**
     * Divide d by k and return the long value closest to the result.
     *
     * @param d divide d by k
     * @param k divide d by k
     * @return the long value closest to the result
     */
    public static long divideByAndRoundToLong(double d, int k) {
        return (long) Math.rint(d / k);
    }

    /**
     * Divide value by factor, take the smallest integer greater than or equal to the result,
     * multiply that integer by factor, and return it.
     *
     * @param value  normalize this value by factor
     * @param factor normalize this value by factor
     * @return the result of value being normalized by factor
     */
    public static int normalize(int value, int factor) {
        return divideByAndCeilToInt(value, factor) * factor;
    }

    /**
     * Divide value by factor, take the smallest integer greater than or equal to the result,
     * multiply that integer by factor, and return it.
     *
     * @param value  normalize this value by factor
     * @param factor normalize this value by factor
     * @return the result of value being normalized by factor
     */
    public static long normalize(long value, int factor) {
        return divideByAndCeilToLong(value, factor) * factor;
    }

    public static String bytesToHex(byte[] in) {
        final char[] hexArray = "0123456789abcdef".toCharArray();

        char[] hexChars = new char[in.length * 2];
        for (int j = 0; j < in.length; j++) {
            int v = in[j] & 0xff;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0xf];
        }
        return new String(hexChars);
    }

    /**
     * Compares two integers
     *
     * @param i1 First number to compare with second one
     * @param i2 Second number to compare with first one
     * @return +1 if i1 &gt; i2, -1 if i2 &gt; i1, 0 if i1 and i2 are equals
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
     * @return +1 if l1 &gt; l2, -1 if l2 &gt; l1, 0 if l1 and l2 are equals
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
