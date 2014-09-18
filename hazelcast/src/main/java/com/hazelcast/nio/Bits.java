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

package com.hazelcast.nio;

/**
 * Access and manipulate bits, bytes, primitives ...
 */
public final class Bits {

    private Bits() {
    }

    public static char readChar(byte[] buffer, int pos, boolean bigEndian) {
        return bigEndian ? readCharB(buffer, pos) : readCharL(buffer, pos);
    }

    public static char readCharB(byte[] buffer, int pos) {
        int byte1 = buffer[pos] & 0xFF;
        int byte0 = buffer[pos + 1] & 0xFF;
        return (char) ((byte1 << 8) + byte0);
    }

    public static char readCharL(byte[] buffer, int pos) {
        int byte1 = buffer[pos] & 0xFF;
        int byte0 = buffer[pos + 1] & 0xFF;
        return (char) ((byte0 << 8) + byte1);
    }

    public static void writeChar(byte[] buffer, int pos, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharB(buffer, pos, v);
        } else {
            writeCharL(buffer, pos, v);
        }
    }

    public static void writeCharB(byte[] buffer, int pos, char v) {
        buffer[pos] = (byte) ((v >>> 8) & 0xFF);
        buffer[pos + 1] = (byte) ((v) & 0xFF);
    }

    public static void writeCharL(byte[] buffer, int pos, char v) {
        buffer[pos] = (byte) ((v) & 0xFF);
        buffer[pos + 1] = (byte) ((v >>> 8) & 0xFF);
    }

    public static short readShort(byte[] buffer, int pos, boolean bigEndian) {
        return bigEndian ? readShortB(buffer, pos) : readShortL(buffer, pos);
    }

    public static short readShortB(byte[] buffer, int pos) {
        int byte1 = buffer[pos] & 0xFF;
        int byte0 = buffer[pos + 1] & 0xFF;
        return (short) ((byte1 << 8) + byte0);
    }

    public static short readShortL(byte[] buffer, int pos) {
        int byte1 = buffer[pos] & 0xFF;
        int byte0 = buffer[pos + 1] & 0xFF;
        return (short) ((byte0 << 8) + byte1);
    }

    public static void writeShort(byte[] buffer, int pos, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortB(buffer, pos, v);
        } else {
            writeShortL(buffer, pos, v);
        }
    }

    public static void writeShortB(byte[] buffer, int pos, short v) {
        buffer[pos] = (byte) ((v >>> 8) & 0xFF);
        buffer[pos + 1] = (byte) ((v) & 0xFF);
    }

    public static void writeShortL(byte[] buffer, int pos, short v) {
        buffer[pos] = (byte) ((v) & 0xFF);
        buffer[pos + 1] = (byte) ((v >>> 8) & 0xFF);
    }

    public static int readInt(byte[] buffer, int pos, boolean bigEndian) {
        if (bigEndian) {
            return readIntB(buffer, pos);
        } else {
            return readIntL(buffer, pos);
        }
    }

    public static int readIntB(byte[] buffer, int pos) {
        int byte3 = (buffer[pos] & 0xFF) << 24;
        int byte2 = (buffer[pos + 1] & 0xFF) << 16;
        int byte1 = (buffer[pos + 2] & 0xFF) << 8;
        int byte0 = buffer[pos + 3] & 0xFF;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static int readIntL(byte[] buffer, int pos) {
        int byte3 = buffer[pos] & 0xFF;
        int byte2 = (buffer[pos + 1] & 0xFF) << 8;
        int byte1 = (buffer[pos + 2] & 0xFF) << 16;
        int byte0 = (buffer[pos + 3] & 0xFF) << 24;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static void writeInt(byte[] buffer, int pos, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntB(buffer, pos, v);
        } else {
            writeIntL(buffer, pos, v);
        }
    }

    public static void writeIntB(byte[] buffer, int pos, int v) {
        buffer[pos] = (byte) ((v >>> 24) & 0xFF);
        buffer[pos + 1] = (byte) ((v >>> 16) & 0xFF);
        buffer[pos + 2] = (byte) ((v >>> 8) & 0xFF);
        buffer[pos + 3] = (byte) ((v) & 0xFF);
    }

    public static void writeIntL(byte[] buffer, int pos, int v) {
        buffer[pos] = (byte) ((v) & 0xFF);
        buffer[pos + 1] = (byte) ((v >>> 8) & 0xFF);
        buffer[pos + 2] = (byte) ((v >>> 16) & 0xFF);
        buffer[pos + 3] = (byte) ((v >>> 24) & 0xFF);
    }

    public static long readLong(byte[] buffer, int pos, boolean bigEndian) {
        if (bigEndian) {
            return readLongB(buffer, pos);
        } else {
            return readLongL(buffer, pos);
        }
    }

    public static long readLongB(byte[] buffer, int pos) {
        long byte7 = (long) buffer[pos] << 56;
        long byte6 = (long) (buffer[pos + 1] & 0xFF) << 48;
        long byte5 = (long) (buffer[pos + 2] & 0xFF) << 40;
        long byte4 = (long) (buffer[pos + 3] & 0xFF) << 32;
        long byte3 = (long) (buffer[pos + 4] & 0xFF) << 24;
        long byte2 = (long) (buffer[pos + 5] & 0xFF) << 16;
        long byte1 = (long) (buffer[pos + 6] & 0xFF) << 8;
        long byte0 = (long) (buffer[pos + 7] & 0xFF);
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static long readLongL(byte[] buffer, int pos) {
        long byte7 = (long) (buffer[pos] & 0xFF);
        long byte6 = (long) (buffer[pos + 1] & 0xFF) << 8;
        long byte5 = (long) (buffer[pos + 2] & 0xFF) << 16;
        long byte4 = (long) (buffer[pos + 3] & 0xFF) << 24;
        long byte3 = (long) (buffer[pos + 4] & 0xFF) << 32;
        long byte2 = (long) (buffer[pos + 5] & 0xFF) << 40;
        long byte1 = (long) (buffer[pos + 6] & 0xFF) << 48;
        long byte0 = (long) (buffer[pos + 7] & 0xFF) << 56;
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static void writeLong(byte[] buffer, int pos, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongB(buffer, pos, v);
        } else {
            writeLongL(buffer, pos, v);
        }
    }

    public static void writeLongB(byte[] buffer, int pos, long v) {
        buffer[pos] = (byte) (v >>> 56);
        buffer[pos + 1] = (byte) (v >>> 48);
        buffer[pos + 2] = (byte) (v >>> 40);
        buffer[pos + 3] = (byte) (v >>> 32);
        buffer[pos + 4] = (byte) (v >>> 24);
        buffer[pos + 5] = (byte) (v >>> 16);
        buffer[pos + 6] = (byte) (v >>> 8);
        buffer[pos + 7] = (byte) (v);
    }

    public static void writeLongL(byte[] buffer, int pos, long v) {
        buffer[pos] = (byte) (v);
        buffer[pos + 1] = (byte) (v >>> 8);
        buffer[pos + 2] = (byte) (v >>> 16);
        buffer[pos + 3] = (byte) (v >>> 24);
        buffer[pos + 4] = (byte) (v >>> 32);
        buffer[pos + 5] = (byte) (v >>> 40);
        buffer[pos + 6] = (byte) (v >>> 48);
        buffer[pos + 7] = (byte) (v >>> 56);
    }

    /**
     * Sets n-th bit of the byte value
     *
     * @param value byte value
     * @param bit n-th bit
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
     * @param bit n-th bit
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
     * @param bit n-th bit
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
     * @param bit n-th bit
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
     * @param bit n-th bit
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
     * @param bit n-th bit
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
