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

package com.hazelcast.internal.tpc.nio;

/**
 * Access and manipulate bits, bytes, primitives...
 */
@SuppressWarnings({"checkstyle:magicnumber", "checkstyle:methodcount"})
public final class Bits {

    /**
     * Byte size in bytes
     */
    public static final int BYTE_SIZE_IN_BYTES = 1;
    /**
     * Boolean size in bytes
     */
    public static final int BOOLEAN_SIZE_IN_BYTES = 1;
    /**
     * Short size in bytes
     */
    public static final int SHORT_SIZE_IN_BYTES = 2;
    /**
     * Char size in bytes
     */
    public static final int CHAR_SIZE_IN_BYTES = 2;
    public static final int BYTES_CHAR = 2;
    /**
     * Integer size in bytes
     */
    public static final int INT_SIZE_IN_BYTES = 4;

    /**
     * Integer size in bytes
     */
    public static final int BYTES_INT = INT_SIZE_IN_BYTES;


    /**
     * Float size in bytes
     */
    public static final int FLOAT_SIZE_IN_BYTES = 4;
    /**
     * Long size in bytes
     */
    public static final int LONG_SIZE_IN_BYTES = 8;
    public static final int BYTES_LONG = LONG_SIZE_IN_BYTES;
    /**
     * Double size in bytes
     */
    public static final int DOUBLE_SIZE_IN_BYTES = 8;
    /**
     * for null arrays, this value is written to the stream to represent null array size.
     */
    public static final int NULL_ARRAY_LENGTH = -1;
    /**
     * Length of the data blocks used by the CPU cache sub-system in bytes.
     */
    public static final int CACHE_LINE_LENGTH = 64;

    private Bits() {
    }

    /**
     * Sets n-th bit of the byte value
     *
     * @param value byte value
     * @param bit   n-th bit
     * @return value
     */
    public static byte setBit(byte value, int bit) {
        value |= 1 << bit;
        return value;
    }

    /**
     * Clears n-th bit of the byte value
     *
     * @param value byte value
     * @param bit   n-th bit
     * @return value
     */
    public static byte clearBit(byte value, int bit) {
        value &= ~(1 << bit);
        return value;
    }

    /**
     * Inverts n-th bit of the byte value
     *
     * @param value byte value
     * @param bit   n-th bit
     * @return value
     */
    public static byte invertBit(byte value, int bit) {
        value ^= 1 << bit;
        return value;
    }

    /**
     * Sets n-th bit of the integer value
     *
     * @param value integer value
     * @param bit   n-th bit
     * @return value
     */
    public static int setBit(int value, int bit) {
        value |= 1 << bit;
        return value;
    }

    /**
     * Clears n-th bit of the integer value
     *
     * @param value integer value
     * @param bit   n-th bit
     * @return value
     */
    public static int clearBit(int value, int bit) {
        value &= ~(1 << bit);
        return value;
    }

    /**
     * Inverts n-th bit of the integer value
     *
     * @param value integer value
     * @param bit   n-th bit
     * @return value
     */
    public static int invertBit(int value, int bit) {
        value ^= 1 << bit;
        return value;
    }

    /**
     * Returns true if n-th bit of the value is set, false otherwise
     */
    public static boolean isBitSet(int value, int bit) {
        return (value & 1 << bit) != 0;
    }

    /**
     * Combines two short integer values into an integer.
     */
    public static int combineToInt(short x, short y) {
        return ((int) x << 16) | ((int) y & 0xFFFF);
    }

    public static short extractShort(int value, boolean lowerBits) {
        return (short) ((lowerBits) ? value : (value >> 16));
    }

    /**
     * Combines two integer values into a long integer.
     */
    public static long combineToLong(int x, int y) {
        return ((long) x << 32) | ((long) y & 0xFFFFFFFFL);
    }

    public static int extractInt(long value, boolean lowerBits) {
        return (int) ((lowerBits) ? value : (value >> 32));
    }
}
