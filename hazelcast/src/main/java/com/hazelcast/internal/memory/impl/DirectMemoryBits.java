/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.memory.impl;

import sun.misc.Unsafe;

/**
 * Utility class to read/write bits to given location (by base object/offset or native memory address)
 * by specified byte order (little/big endian).
 */
public final class DirectMemoryBits {

    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;
    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE != null ? UNSAFE.arrayBaseOffset(byte[].class) : -1;

    private DirectMemoryBits() {
    }

    //////////////////////////////////////////////////////////////////

    public static char readChar(long address, boolean bigEndian) {
        return readChar(null, address, bigEndian);
    }

    public static char readChar(byte[] buffer, int pos, boolean bigEndian) {
        return readChar(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static char readChar(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readCharB(base, offset);
        } else {
            return readCharL(base, offset);
        }
    }

    public static char readCharB(Object base, long offset) {
        int byte1 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte0 = UNSAFE.getByte(base, offset + 1) & 0xFF;
        return (char) ((byte1 << 8) + byte0);
    }

    public static char readCharL(Object base, long offset) {
        int byte1 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte0 = UNSAFE.getByte(base, offset + 1) & 0xFF;
        return (char) ((byte0 << 8) + byte1);
    }

    public static void writeChar(long address, char v, boolean bigEndian) {
        writeChar(null, address, v, bigEndian);
    }

    public static void writeChar(byte[] buffer, int pos, char v, boolean bigEndian) {
        writeChar(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeChar(Object base, long offset, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharB(base, offset, v);
        } else {
            writeCharL(base, offset, v);
        }
    }

    public static void writeCharB(Object base, long offset, char v) {
        UNSAFE.putByte(base, offset, (byte) ((v >>> 8) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v) & 0xFF));
    }

    public static void writeCharL(Object base, long offset, char v) {
        UNSAFE.putByte(base, offset, (byte) ((v) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static short readShort(long address, boolean bigEndian) {
        return readShort(null, address, bigEndian);
    }

    public static short readShort(byte[] buffer, int pos, boolean bigEndian) {
        return readShort(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static short readShort(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readShortB(base, offset);
        } else {
            return readShortL(base, offset);
        }
    }

    public static short readShortB(Object base, long offset) {
        int byte1 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte0 = UNSAFE.getByte(base, offset + 1) & 0xFF;
        return (short) ((byte1 << 8) + byte0);
    }

    public static short readShortL(Object base, long offset) {
        int byte1 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte0 = UNSAFE.getByte(base, offset + 1) & 0xFF;
        return (short) ((byte0 << 8) + byte1);
    }

    public static void writeShort(long address, short v, boolean bigEndian) {
        writeShort(null, address, v, bigEndian);
    }

    public static void writeShort(byte[] buffer, int pos, short v, boolean bigEndian) {
        writeShort(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeShort(Object base, long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortB(base, offset, v);
        } else {
            writeShortL(base, offset, v);
        }
    }

    public static void writeShortB(Object base, long offset, short v) {
        UNSAFE.putByte(base, offset, (byte) ((v >>> 8) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v) & 0xFF));
    }

    public static void writeShortL(Object base, long offset, short v) {
        UNSAFE.putByte(base, offset, (byte) ((v) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static int readInt(long address, boolean bigEndian) {
        return readInt(null, address, bigEndian);
    }

    public static int readInt(byte[] buffer, int pos, boolean bigEndian) {
        return readInt(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static int readInt(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readIntB(base, offset);
        } else {
            return readIntL(base, offset);
        }
    }

    public static int readIntB(Object base, long offset) {
        int byte3 = (UNSAFE.getByte(base, offset) & 0xFF) << 24;
        int byte2 = (UNSAFE.getByte(base, offset + 1) & 0xFF) << 16;
        int byte1 = (UNSAFE.getByte(base, offset + 2) & 0xFF) << 8;
        int byte0 = UNSAFE.getByte(base, offset + 3) & 0xFF;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static int readIntL(Object base, long offset) {
        int byte3 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte2 = (UNSAFE.getByte(base, offset + 1) & 0xFF) << 8;
        int byte1 = (UNSAFE.getByte(base, offset + 2) & 0xFF) << 16;
        int byte0 = (UNSAFE.getByte(base, offset + 3) & 0xFF) << 24;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static void writeInt(long address, int v, boolean bigEndian) {
        writeInt(null, address, v, bigEndian);
    }

    public static void writeInt(byte[] buffer, int pos, int v, boolean bigEndian) {
        writeInt(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeInt(Object base, long offset, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntB(base, offset, v);
        } else {
            writeIntL(base, offset, v);
        }
    }

    public static void writeIntB(Object base, long offset, int v) {
        UNSAFE.putByte(base, offset, (byte) ((v >>> 24) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v >>> 16) & 0xFF));
        UNSAFE.putByte(base, offset + 2, (byte) ((v >>> 8) & 0xFF));
        UNSAFE.putByte(base, offset + 3, (byte) ((v) & 0xFF));
    }

    public static void writeIntL(Object base, long offset, int v) {
        UNSAFE.putByte(base, offset, (byte) ((v) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
        UNSAFE.putByte(base, offset + 2, (byte) ((v >>> 16) & 0xFF));
        UNSAFE.putByte(base, offset + 3, (byte) ((v >>> 24) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static float readFloat(long address, boolean bigEndian) {
        return readFloat(null, address, bigEndian);
    }

    public static float readFloat(byte[] buffer, int pos, boolean bigEndian) {
        return readFloat(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static float readFloat(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readFloatB(base, offset);
        } else {
            return readFloatL(base, offset);
        }
    }

    public static float readFloatB(Object base, long offset) {
        return Float.intBitsToFloat(readIntB(base, offset));
    }

    public static float readFloatL(Object base, long offset) {
        return Float.intBitsToFloat(readIntL(base, offset));
    }

    public static void writeFloat(long address, float v, boolean bigEndian) {
        writeFloat(null, address, v, bigEndian);
    }

    public static void writeFloat(byte[] buffer, int pos, float v, boolean bigEndian) {
        writeFloat(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeFloat(Object base, long offset, float v, boolean bigEndian) {
        if (bigEndian) {
            writeFloatB(base, offset, v);
        } else {
            writeFloatL(base, offset, v);
        }
    }

    public static void writeFloatB(Object base, long offset, float v) {
        writeIntB(base, offset, Float.floatToRawIntBits(v));
    }

    public static void writeFloatL(Object base, long offset, float v) {
        writeIntL(base, offset, Float.floatToRawIntBits(v));
    }

    //////////////////////////////////////////////////////////////////

    public static long readLong(long address, boolean bigEndian) {
        return readLong(null, address, bigEndian);
    }

    public static long readLong(byte[] buffer, int pos, boolean bigEndian) {
        return readLong(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static long readLong(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readLongB(base, offset);
        } else {
            return readLongL(base, offset);
        }
    }

    public static long readLongB(Object base, long offset) {
        long byte7 = (long) UNSAFE.getByte(base, offset) << 56;
        long byte6 = (long) (UNSAFE.getByte(base, offset + 1) & 0xFF) << 48;
        long byte5 = (long) (UNSAFE.getByte(base, offset + 2) & 0xFF) << 40;
        long byte4 = (long) (UNSAFE.getByte(base, offset + 3) & 0xFF) << 32;
        long byte3 = (long) (UNSAFE.getByte(base, offset + 4) & 0xFF) << 24;
        long byte2 = (long) (UNSAFE.getByte(base, offset + 5) & 0xFF) << 16;
        long byte1 = (long) (UNSAFE.getByte(base, offset + 6) & 0xFF) << 8;
        long byte0 = (long) (UNSAFE.getByte(base, offset + 7) & 0xFF);
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static long readLongL(Object base, long offset) {
        long byte7 = (long) (UNSAFE.getByte(base, offset) & 0xFF);
        long byte6 = (long) (UNSAFE.getByte(base, offset + 1) & 0xFF) << 8;
        long byte5 = (long) (UNSAFE.getByte(base, offset + 2) & 0xFF) << 16;
        long byte4 = (long) (UNSAFE.getByte(base, offset + 3) & 0xFF) << 24;
        long byte3 = (long) (UNSAFE.getByte(base, offset + 4) & 0xFF) << 32;
        long byte2 = (long) (UNSAFE.getByte(base, offset + 5) & 0xFF) << 40;
        long byte1 = (long) (UNSAFE.getByte(base, offset + 6) & 0xFF) << 48;
        long byte0 = (long) (UNSAFE.getByte(base, offset + 7) & 0xFF) << 56;
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static void writeLong(long address, long v, boolean bigEndian) {
        writeLong(null, address, v, bigEndian);
    }

    public static void writeLong(byte[] buffer, int pos, long v, boolean bigEndian) {
        writeLong(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeLong(Object base, long offset, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongB(base, offset, v);
        } else {
            writeLongL(base, offset, v);
        }
    }

    public static void writeLongB(Object base, long offset, long v) {
        UNSAFE.putByte(base, offset, (byte) (v >>> 56));
        UNSAFE.putByte(base, offset + 1, (byte) (v >>> 48));
        UNSAFE.putByte(base, offset + 2, (byte) (v >>> 40));
        UNSAFE.putByte(base, offset + 3, (byte) (v >>> 32));
        UNSAFE.putByte(base, offset + 4, (byte) (v >>> 24));
        UNSAFE.putByte(base, offset + 5, (byte) (v >>> 16));
        UNSAFE.putByte(base, offset + 6, (byte) (v >>> 8));
        UNSAFE.putByte(base, offset + 7, (byte) (v));
    }

    public static void writeLongL(Object base, long offset, long v) {
        UNSAFE.putByte(base, offset, (byte) (v));
        UNSAFE.putByte(base, offset + 1, (byte) (v >>> 8));
        UNSAFE.putByte(base, offset + 2, (byte) (v >>> 16));
        UNSAFE.putByte(base, offset + 3, (byte) (v >>> 24));
        UNSAFE.putByte(base, offset + 4, (byte) (v >>> 32));
        UNSAFE.putByte(base, offset + 5, (byte) (v >>> 40));
        UNSAFE.putByte(base, offset + 6, (byte) (v >>> 48));
        UNSAFE.putByte(base, offset + 7, (byte) (v >>> 56));
    }

    //////////////////////////////////////////////////////////////////

    public static double readDouble(long address, boolean bigEndian) {
        return readDouble(null, address, bigEndian);
    }

    public static double readDouble(byte[] buffer, int pos, boolean bigEndian) {
        return readDouble(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static double readDouble(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readDoubleB(base, offset);
        } else {
            return readDoubleL(base, offset);
        }
    }

    public static double readDoubleB(Object base, long offset) {
        return Double.longBitsToDouble(readLongB(base, offset));
    }

    public static double readDoubleL(Object base, long offset) {
        return Double.longBitsToDouble(readLongL(base, offset));
    }

    public static void writeDouble(long address, double v, boolean bigEndian) {
        writeDouble(null, address, v, bigEndian);
    }

    public static void writeDouble(byte[] buffer, int pos, double v, boolean bigEndian) {
        writeDouble(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeDouble(Object base, long offset, double v, boolean bigEndian) {
        if (bigEndian) {
            writeDoubleB(base, offset, v);
        } else {
            writeDoubleL(base, offset, v);
        }
    }

    public static void writeDoubleB(Object base, long offset, double v) {
        writeLongB(base, offset, Double.doubleToRawLongBits(v));
    }

    public static void writeDoubleL(Object base, long offset, double v) {
        writeLongL(base, offset, Double.doubleToRawLongBits(v));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static char readCharVolatile(long address, boolean bigEndian) {
        return readCharVolatile(null, address, bigEndian);
    }

    public static char readCharVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readCharVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static char readCharVolatile(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readCharVolatileB(base, offset);
        } else {
            return readCharVolatileL(base, offset);
        }
    }

    public static char readCharVolatileB(Object base, long offset) {
        int byte1 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte0 = UNSAFE.getByte(base, offset + 1) & 0xFF;
        return (char) ((byte1 << 8) + byte0);
    }

    public static char readCharVolatileL(Object base, long offset) {
        int byte1 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte0 = UNSAFE.getByte(base, offset + 1) & 0xFF;
        return (char) ((byte0 << 8) + byte1);
    }

    public static void writeCharVolatile(long address, char v, boolean bigEndian) {
        writeCharVolatile(null, address, v, bigEndian);
    }

    public static void writeCharVolatile(byte[] buffer, int pos, char v, boolean bigEndian) {
        writeCharVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeCharVolatile(Object base, long offset, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharVolatileB(base, offset, v);
        } else {
            writeCharVolatileL(base, offset, v);
        }
    }

    public static void writeCharVolatileB(Object base, long offset, char v) {
        UNSAFE.putByte(base, offset, (byte) ((v >>> 8) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v) & 0xFF));
    }

    public static void writeCharVolatileL(Object base, long offset, char v) {
        UNSAFE.putByte(base, offset, (byte) ((v) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static short readShortVolatile(long address, boolean bigEndian) {
        return readShortVolatile(null, address, bigEndian);
    }

    public static short readShortVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readShortVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static short readShortVolatile(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readShortVolatileB(base, offset);
        } else {
            return readShortVolatileL(base, offset);
        }
    }

    public static short readShortVolatileB(Object base, long offset) {
        int byte1 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte0 = UNSAFE.getByte(base, offset + 1) & 0xFF;
        return (short) ((byte1 << 8) + byte0);
    }

    public static short readShortVolatileL(Object base, long offset) {
        int byte1 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte0 = UNSAFE.getByte(base, offset + 1) & 0xFF;
        return (short) ((byte0 << 8) + byte1);
    }

    public static void writeShortVolatile(long address, short v, boolean bigEndian) {
        writeShortVolatile(null, address, v, bigEndian);
    }

    public static void writeShortVolatile(byte[] buffer, int pos, short v, boolean bigEndian) {
        writeShortVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeShortVolatile(Object base, long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortVolatileB(base, offset, v);
        } else {
            writeShortVolatileL(base, offset, v);
        }
    }

    public static void writeShortVolatileB(Object base, long offset, short v) {
        UNSAFE.putByte(base, offset, (byte) ((v >>> 8) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v) & 0xFF));
    }

    public static void writeShortVolatileL(Object base, long offset, short v) {
        UNSAFE.putByte(base, offset, (byte) ((v) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static int readIntVolatile(long address, boolean bigEndian) {
        return readIntVolatile(null, address, bigEndian);
    }

    public static int readIntVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readIntVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static int readIntVolatile(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readIntVolatileB(base, offset);
        } else {
            return readIntVolatileL(base, offset);
        }
    }

    public static int readIntVolatileB(Object base, long offset) {
        int byte3 = (UNSAFE.getByte(base, offset) & 0xFF) << 24;
        int byte2 = (UNSAFE.getByte(base, offset + 1) & 0xFF) << 16;
        int byte1 = (UNSAFE.getByte(base, offset + 2) & 0xFF) << 8;
        int byte0 = UNSAFE.getByte(base, offset + 3) & 0xFF;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static int readIntVolatileL(Object base, long offset) {
        int byte3 = UNSAFE.getByte(base, offset) & 0xFF;
        int byte2 = (UNSAFE.getByte(base, offset + 1) & 0xFF) << 8;
        int byte1 = (UNSAFE.getByte(base, offset + 2) & 0xFF) << 16;
        int byte0 = (UNSAFE.getByte(base, offset + 3) & 0xFF) << 24;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static void writeIntVolatile(long address, int v, boolean bigEndian) {
        writeIntVolatile(null, address, v, bigEndian);
    }

    public static void writeIntVolatile(byte[] buffer, int pos, int v, boolean bigEndian) {
        writeIntVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeIntVolatile(Object base, long offset, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntVolatileB(base, offset, v);
        } else {
            writeIntVolatileL(base, offset, v);
        }
    }

    public static void writeIntVolatileB(Object base, long offset, int v) {
        UNSAFE.putByte(base, offset, (byte) ((v >>> 24) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v >>> 16) & 0xFF));
        UNSAFE.putByte(base, offset + 2, (byte) ((v >>> 8) & 0xFF));
        UNSAFE.putByte(base, offset + 3, (byte) ((v) & 0xFF));
    }

    public static void writeIntVolatileL(Object base, long offset, int v) {
        UNSAFE.putByte(base, offset, (byte) ((v) & 0xFF));
        UNSAFE.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
        UNSAFE.putByte(base, offset + 2, (byte) ((v >>> 16) & 0xFF));
        UNSAFE.putByte(base, offset + 3, (byte) ((v >>> 24) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static float readFloatVolatile(long address, boolean bigEndian) {
        return readFloatVolatile(null, address, bigEndian);
    }

    public static float readFloatVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readFloatVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static float readFloatVolatile(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readFloatVolatileB(base, offset);
        } else {
            return readFloatVolatileL(base, offset);
        }
    }

    public static float readFloatVolatileB(Object base, long offset) {
        return Float.intBitsToFloat(readIntVolatileB(base, offset));
    }

    public static float readFloatVolatileL(Object base, long offset) {
        return Float.intBitsToFloat(readIntVolatileL(base, offset));
    }

    public static void writeFloatVolatile(long address, float v, boolean bigEndian) {
        writeFloatVolatile(null, address, v, bigEndian);
    }

    public static void writeFloatVolatile(byte[] buffer, int pos, float v, boolean bigEndian) {
        writeFloatVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeFloatVolatile(Object base, long offset, float v, boolean bigEndian) {
        if (bigEndian) {
            writeFloatVolatileB(base, offset, v);
        } else {
            writeFloatVolatileL(base, offset, v);
        }
    }

    public static void writeFloatVolatileB(Object base, long offset, float v) {
        writeIntVolatileB(base, offset, Float.floatToRawIntBits(v));
    }

    public static void writeFloatVolatileL(Object base, long offset, float v) {
        writeIntVolatileL(base, offset, Float.floatToRawIntBits(v));
    }

    //////////////////////////////////////////////////////////////////

    public static long readLongVolatile(long address, boolean bigEndian) {
        return readLongVolatile(null, address, bigEndian);
    }

    public static long readLongVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readLongVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static long readLongVolatile(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readLongVolatileB(base, offset);
        } else {
            return readLongVolatileL(base, offset);
        }
    }

    public static long readLongVolatileB(Object base, long offset) {
        long byte7 = (long) UNSAFE.getByte(base, offset) << 56;
        long byte6 = (long) (UNSAFE.getByte(base, offset + 1) & 0xFF) << 48;
        long byte5 = (long) (UNSAFE.getByte(base, offset + 2) & 0xFF) << 40;
        long byte4 = (long) (UNSAFE.getByte(base, offset + 3) & 0xFF) << 32;
        long byte3 = (long) (UNSAFE.getByte(base, offset + 4) & 0xFF) << 24;
        long byte2 = (long) (UNSAFE.getByte(base, offset + 5) & 0xFF) << 16;
        long byte1 = (long) (UNSAFE.getByte(base, offset + 6) & 0xFF) << 8;
        long byte0 = (long) (UNSAFE.getByte(base, offset + 7) & 0xFF);
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static long readLongVolatileL(Object base, long offset) {
        long byte7 = (long) (UNSAFE.getByte(base, offset) & 0xFF);
        long byte6 = (long) (UNSAFE.getByte(base, offset + 1) & 0xFF) << 8;
        long byte5 = (long) (UNSAFE.getByte(base, offset + 2) & 0xFF) << 16;
        long byte4 = (long) (UNSAFE.getByte(base, offset + 3) & 0xFF) << 24;
        long byte3 = (long) (UNSAFE.getByte(base, offset + 4) & 0xFF) << 32;
        long byte2 = (long) (UNSAFE.getByte(base, offset + 5) & 0xFF) << 40;
        long byte1 = (long) (UNSAFE.getByte(base, offset + 6) & 0xFF) << 48;
        long byte0 = (long) (UNSAFE.getByte(base, offset + 7) & 0xFF) << 56;
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static void writeLongVolatile(long address, long v, boolean bigEndian) {
        writeLongVolatile(null, address, v, bigEndian);
    }

    public static void writeLongVolatile(byte[] buffer, int pos, long v, boolean bigEndian) {
        writeLongVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeLongVolatile(Object base, long offset, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongVolatileB(base, offset, v);
        } else {
            writeLongVolatileL(base, offset, v);
        }
    }

    public static void writeLongVolatileB(Object base, long offset, long v) {
        UNSAFE.putByte(base, offset, (byte) (v >>> 56));
        UNSAFE.putByte(base, offset + 1, (byte) (v >>> 48));
        UNSAFE.putByte(base, offset + 2, (byte) (v >>> 40));
        UNSAFE.putByte(base, offset + 3, (byte) (v >>> 32));
        UNSAFE.putByte(base, offset + 4, (byte) (v >>> 24));
        UNSAFE.putByte(base, offset + 5, (byte) (v >>> 16));
        UNSAFE.putByte(base, offset + 6, (byte) (v >>> 8));
        UNSAFE.putByte(base, offset + 7, (byte) (v));
    }

    public static void writeLongVolatileL(Object base, long offset, long v) {
        UNSAFE.putByte(base, offset, (byte) (v));
        UNSAFE.putByte(base, offset + 1, (byte) (v >>> 8));
        UNSAFE.putByte(base, offset + 2, (byte) (v >>> 16));
        UNSAFE.putByte(base, offset + 3, (byte) (v >>> 24));
        UNSAFE.putByte(base, offset + 4, (byte) (v >>> 32));
        UNSAFE.putByte(base, offset + 5, (byte) (v >>> 40));
        UNSAFE.putByte(base, offset + 6, (byte) (v >>> 48));
        UNSAFE.putByte(base, offset + 7, (byte) (v >>> 56));
    }

    //////////////////////////////////////////////////////////////////

    public static double readDoubleVolatile(long address, boolean bigEndian) {
        return readDoubleVolatile(null, address, bigEndian);
    }

    public static double readDoubleVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readDoubleVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), bigEndian);
    }

    public static double readDoubleVolatile(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readDoubleVolatileB(base, offset);
        } else {
            return readDoubleVolatileL(base, offset);
        }
    }

    public static double readDoubleVolatileB(Object base, long offset) {
        return Double.longBitsToDouble(readLongVolatileB(base, offset));
    }

    public static double readDoubleVolatileL(Object base, long offset) {
        return Double.longBitsToDouble(readLongVolatileL(base, offset));
    }

    public static void writeDoubleVolatile(long address, double v, boolean bigEndian) {
        writeDoubleVolatile(null, address, v, bigEndian);
    }

    public static void writeDoubleVolatile(byte[] buffer, int pos, double v, boolean bigEndian) {
        writeDoubleVolatile(buffer, (long) (BYTE_ARRAY_BASE_OFFSET + pos), v, bigEndian);
    }

    public static void writeDoubleVolatile(Object base, long offset, double v, boolean bigEndian) {
        if (bigEndian) {
            writeDoubleVolatileB(base, offset, v);
        } else {
            writeDoubleVolatileL(base, offset, v);
        }
    }

    public static void writeDoubleVolatileB(Object base, long offset, double v) {
        writeLongVolatileB(base, offset, Double.doubleToRawLongBits(v));
    }

    public static void writeDoubleVolatileL(Object base, long offset, double v) {
        writeLongVolatileL(base, offset, Double.doubleToRawLongBits(v));
    }

}
