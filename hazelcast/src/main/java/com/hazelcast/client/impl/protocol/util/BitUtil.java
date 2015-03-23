/*
 * Copyright 2014 Real Logic Ltd.
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
package com.hazelcast.client.impl.protocol.util;

import java.nio.charset.Charset;

/**
 * Miscellaneous useful functions for dealing with low level bits and bytes.
 */
public final class BitUtil {
    /**
     * Size of a byte in bytes
     */
    public static final int SIZE_OF_BYTE = 1;
    /**
     * Size of a boolean in bytes
     */
    public static final int SIZE_OF_BOOLEAN = 1;
    /**
     * Size of a char in bytes
     */
    public static final int SIZE_OF_CHAR = 2;
    /**
     * Size of a short in bytes
     */
    public static final int SIZE_OF_SHORT = 2;
    /**
     * Size of an int in bytes
     */
    public static final int SIZE_OF_INT = 4;
    /**
     * Size of a a float in bytes
     */
    public static final int SIZE_OF_FLOAT = 4;
    /**
     * Size of a long in bytes
     */
    public static final int SIZE_OF_LONG = 8;
    /**
     * Size of a double in bytes
     */
    public static final int SIZE_OF_DOUBLE = 8;
    /**
     * Length of the data blocks used by the CPU cache sub-system in bytes.
     */
    public static final int CACHE_LINE_LENGTH = 64;
    /**
     * Long mask
     */
    public static final long LONG_MASK = 0xFFFFFFFFL;
    /**
     * Int mask
     */
    public static final int INT_MASK = 0xFFFF;
    /**
     * byte mask
     */
    public static final short BYTE_MASK = 0xFF;
    /**
     * UTF-8 charset
     */
    public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private static final int LAST_DIGIT_MASK = 1;
    private static final int UINT_BIT_SIZE = 32;

    private BitUtil() {
    }

    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value.
     * <p/>
     * If the value is &lt;= 0 then 1 will be returned.
     * <p/>
     * This method is not suitable for {@link Integer#MIN_VALUE} or numbers greater than 2^30.
     *
     * @param value from which to search for next power of 2
     * @return The next power of 2 or the value itself if it is a power of 2
     */
    public static int findNextPositivePowerOfTwo(final int value) {
        return 1 << (UINT_BIT_SIZE - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * Align a value to the next multiple up of alignment.
     * If the value equals an alignment multiple then it is returned unchanged.
     * <p/>
     * This method executes without branching.
     *
     * @param value     to be aligned up.
     * @param alignment to be used.
     * @return the value aligned to the next boundary.
     */
    public static int align(final int value, final int alignment) {
        return (value + (alignment - 1)) & ~(alignment - 1);
    }

    /**
     * Is a number even.
     *
     * @param value to check.
     * @return true if the number is even otherwise false.
     */
    public static boolean isEven(final int value) {
        return (value & LAST_DIGIT_MASK) == 0;
    }

    /**
     * Is a value a positive power of two.
     *
     * @param value to be checked.
     * @return true if the number is a positive power of two otherwise false.
     */
    public static boolean isPowerOfTwo(final int value) {
        return value > 0 && ((value & (~value + 1)) == value);
    }

    /**
     * Cycles indices of an array one at a time in a forward fashion
     *
     * @param current value to be incremented.
     * @param max     value for the cycle.
     * @return the next value, or zero if max is reached.
     */
    public static int next(final int current, final int max) {
        int next = current + 1;
        if (next == max) {
            next = 0;
        }

        return next;
    }

    /**
     * Cycles indices of an array one at a time in a backwards fashion
     *
     * @param current value to be decremented.
     * @param max     value of the cycle.
     * @return the next value, or max - 1 if current is zero
     */
    public static int previous(final int current, final int max) {
        if (0 == current) {
            return max - 1;
        }

        return current - 1;
    }

//    /**
//     * Calculate the shift value to scale a number based on how refs are compressed or not.
//     *
//     * @param scale of the number reported by Unsafe.
//     * @return how many times the number needs to be shifted to the left.
//     */
//    public static int calculateShiftForScale(final int scale) {
//        if (4 == scale) {
//            return 2;
//        } else if (8 == scale) {
//            return 3;
//        } else {
//            throw new IllegalArgumentException("Unknown pointer size");
//        }
//    }

}
