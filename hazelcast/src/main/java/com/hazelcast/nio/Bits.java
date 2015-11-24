/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.charset.Charset;

/**
 * Access and manipulate bits, bytes, primitives ...
 */
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
    /**
     * Integer size in bytes
     */
    public static final int INT_SIZE_IN_BYTES = 4;
    /**
     * Float size in bytes
     */
    public static final int FLOAT_SIZE_IN_BYTES = 4;
    /**
     * Long size in bytes
     */
    public static final int LONG_SIZE_IN_BYTES = 8;
    /**
     * Double size in bytes
     */
    public static final int DOUBLE_SIZE_IN_BYTES = 8;
    /**
     * for null arrays, this value writen to stream to represent null array size.
     */
    public static final int NULL_ARRAY_LENGTH = -1;
    /**
     * Length of the data blocks used by the CPU cache sub-system in bytes.
     */
    public static final int CACHE_LINE_LENGTH = 64;

    /**
     * A reusable instance of the UTF-8 charset
     * */
    public static final Charset UTF_8 = Charset.forName("UTF-8");
    /**
     * A reusable instance of the ISO Latin-1 charset
     * */
    public static final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");

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

    public static int writeUtf8Char(byte[] buffer, int pos, int c) {
        if (c <= 0x007F) {
            buffer[pos] = (byte) c;
            return 1;
        } else if (c > 0x07FF) {
            buffer[pos] = (byte) (0xE0 | c >> 12 & 0x0F);
            buffer[pos + 1] = (byte) (0x80 | c >> 6 & 0x3F);
            buffer[pos + 2] = (byte) (0x80 | c & 0x3F);
            return 3;
        } else {
            buffer[pos] = (byte) (0xC0 | c >> 6 & 0x1F);
            buffer[pos + 1] = (byte) (0x80 | c & 0x3F);
            return 2;
        }
    }

    public static int readUtf8Char(byte[] buffer, int pos, char[] dst, int dstPos)
            throws IOException {
        int b = buffer[pos] & 0xFF;
        switch (b >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                dst[dstPos] = (char) b;
                return 1;
            case 12:
            case 13:
                int first = (b & 0x1F) << 6;
                int second = buffer[pos + 1] & 0x3F;
                dst[dstPos] = (char) (first | second);
                return 2;
            case 14:
                int first2 = (b & 0x0F) << 12;
                int second2 = (buffer[pos + 1] & 0x3F) << 6;
                int third2 = buffer[pos + 2] & 0x3F;
                dst[dstPos] = (char) (first2 | second2 | third2);
                return 3;
            default:
                throw new UTFDataFormatException("Malformed byte sequence");
        }
    }

    public static char readUtf8Char(DataInput in, byte firstByte)
            throws IOException {
        int b = firstByte & 0xFF;
        switch (b >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return (char) b;
            case 12:
            case 13:
                int first = (b & 0x1F) << 6;
                int second = in.readByte() & 0x3F;
                return (char) (first | second);
            case 14:
                int first2 = (b & 0x0F) << 12;
                int second2 = (in.readByte() & 0x3F) << 6;
                int third2 = in.readByte() & 0x3F;
                return (char) (first2 | second2 | third2);
            default:
                throw new UTFDataFormatException("Malformed byte sequence");
        }
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
