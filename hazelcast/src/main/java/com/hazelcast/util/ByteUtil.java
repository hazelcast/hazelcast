/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 * This class allows to set, clear and check bits of a byte value.
 */
public final class ByteUtil {

    /**
     * Array of bit positions.
     */
    private static final byte[] POWERS = new byte[]{
            (byte) (1 << 0),
            (byte) (1 << 1),
            (byte) (1 << 2),
            (byte) (1 << 3),
            (byte) (1 << 4),
            (byte) (1 << 5),
            (byte) (1 << 6),
            (byte) (1 << 7),
    };

    /**
     * All members are static and there should
     * never be an instance of this class.
     */
    private ByteUtil() {
    }

    /**
     * Sets a bit to 1.
     *
     * @param number the original byte value
     * @param index  the bit to set
     * @return the modified byte value
     */
    public static byte setTrue(final byte number, final int index) {
        return (byte) (number | POWERS[index]);
    }

    /**
     * Clears a bit, by setting it to 0.
     *
     * @param number the original byte value
     * @param index  the bit to set
     * @return the modified byte value
     */
    public static byte setFalse(final byte number, final int index) {
        return (byte) (number & ~(POWERS[index]));
    }

    /**
     * Checks if the index-th bit of number is set.
     *
     * @param number the byte value
     * @param index  the bit to check
     * @return true if the bit is set, false otherwise
     */
    public static boolean isTrue(final byte number, final int index) {
        return (number & (POWERS[index])) != 0;
    }

    /**
     * Checks if the index-th bit of number is NOT set.
     *
     * @param number the byte value
     * @param index  the bit to check
     * @return true if the bit is NOT set, false otherwise
     */
    public static boolean isFalse(final byte number, final int index) {
        return (number & (POWERS[index])) == 0;
    }

    public static byte toByte(boolean... values) {
        if (values.length > Byte.SIZE) {
            throw new IllegalArgumentException(
                    "Expected less or equal to " + Byte.SIZE + " arguments");
        }
        byte b = 0;
        for (int i = 0; i < values.length; i++) {
            b |= values[i] ? POWERS[i] : 0;
        }
        return b;
    }

    public static boolean[] fromByte(byte b) {
        boolean[] values = new boolean[Byte.SIZE];
        for (int i = 0; i < values.length; i++) {
            values[i] = (b & POWERS[i]) != 0;
        }
        return values;
    }

    public static String toBinaryString(final byte number) {
        final StringBuilder builder = new StringBuilder(Byte.SIZE);
        for (int i = 0; i < Byte.SIZE; i++) {
            final int q = number & POWERS[Byte.SIZE - 1 - i];
            builder.append(q != 0 ? '1' : '0');
        }
        return builder.toString();
    }

    public static byte[] concatenate(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    public static int combineToInt(short x, short y) {
        return ((int) x << 16) | ((int) y & 0xFFFF);
    }

    public static short extractShort(int value, boolean lowerBits) {
        return (short) ((lowerBits) ? value :(value >> 16));
    }

    public static long combineToLong(int x, int y) {
        return ((long) x << 32) | ((long) y & 0xFFFFFFFL);
    }

    public static int extractInt(long value, boolean lowerBits) {
        return (lowerBits) ? (int) value : (int) (value >> 32);
    }

}
