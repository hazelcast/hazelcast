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

import com.hazelcast.internal.memory.MemoryAccessor;

import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;

/**
 * Utility class to read/write bits to given location (by base object/offset or native memory address)
 * by specified byte order (little/big endian).
 */
public final class DirectMemoryBits {
    private static final MemoryAccessor BYTE_ARRAY_MEMORY_ACCESSOR = MemoryAccessor.HEAP_BYTE_ARRAY_MEM;
    private static final MemoryAccessor STANDARD_MEMORY_ACCESSOR = MemoryAccessor.AMEM;


    private DirectMemoryBits() {
    }

    //////////////////////////////////////////////////////////////////

    public static char readChar(long address, boolean bigEndian) {
        return readChar(null, address, bigEndian);
    }

    public static char readChar(byte[] buffer, int pos, boolean bigEndian) {
        return readChar(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static char readChar(Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readCharB(base, offset);
        } else {
            return readCharL(base, offset);
        }
    }

    public static char readChar(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readCharB(memoryAccessor, base, offset);
        } else {
            return readCharL(memoryAccessor, base, offset);
        }
    }

    public static char readCharB(Object base, long offset) {
        return readCharB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static char readCharL(Object base, long offset) {
        return readCharL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static char readCharB(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte1 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte0 = memoryAccessor.getByte(base, offset + 1) & 0xFF;
        return (char) ((byte1 << 8) + byte0);
    }

    public static char readCharL(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte1 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte0 = memoryAccessor.getByte(base, offset + 1) & 0xFF;
        return (char) ((byte0 << 8) + byte1);
    }

    public static void writeChar(long address, char v, boolean bigEndian) {
        writeChar(null, address, v, bigEndian);
    }

    public static void writeChar(byte[] buffer, int pos, char v, boolean bigEndian) {
        writeChar(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeChar(Object base, long offset, char v, boolean bigEndian) {
        writeChar(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeChar(MemoryAccessor memoryAccessor, Object base, long offset, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharB(memoryAccessor, base, offset, v);
        } else {
            writeCharL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeCharB(MemoryAccessor memoryAccessor, Object base, long offset, char v) {
        memoryAccessor.putByte(base, offset, (byte) ((v >>> 8) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v) & 0xFF));
    }

    public static void writeCharL(MemoryAccessor memoryAccessor, Object base, long offset, char v) {
        memoryAccessor.putByte(base, offset, (byte) ((v) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    public static void writeCharB(Object base, long offset, char v) {
        writeCharB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeCharL(Object base, long offset, char v) {
        writeCharL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    //////////////////////////////////////////////////////////////////

    public static short readShort(long address, boolean bigEndian) {
        return readShort(null, address, bigEndian);
    }

    public static short readShort(byte[] buffer, int pos, boolean bigEndian) {
        return readShort(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static short readShort(Object base, long offset, boolean bigEndian) {
        return readShort(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static short readShort(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readShortB(memoryAccessor, base, offset);
        } else {
            return readShortL(memoryAccessor, base, offset);
        }
    }

    public static short readShortB(Object base, long offset) {
        return readShortB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static short readShortL(Object base, long offset) {
        return readShortL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static short readShortB(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte1 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte0 = memoryAccessor.getByte(base, offset + 1) & 0xFF;
        return (short) ((byte1 << 8) + byte0);
    }

    public static short readShortL(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte1 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte0 = memoryAccessor.getByte(base, offset + 1) & 0xFF;
        return (short) ((byte0 << 8) + byte1);
    }

    public static void writeShort(long address, short v, boolean bigEndian) {
        writeShort(null, address, v, bigEndian);
    }

    public static void writeShort(byte[] buffer, int pos, short v, boolean bigEndian) {
        writeShort(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeShort(Object base, long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortB(base, offset, v);
        } else {
            writeShortL(base, offset, v);
        }
    }

    public static void writeShort(MemoryAccessor memoryAccessor, Object base, long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortB(memoryAccessor, base, offset, v);
        } else {
            writeShortL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeShortB(Object base, long offset, short v) {
        writeShortB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeShortL(Object base, long offset, short v) {
        writeShortL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeShortB(MemoryAccessor memoryAccessor, Object base, long offset, short v) {
        memoryAccessor.putByte(base, offset, (byte) ((v >>> 8) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v) & 0xFF));
    }

    public static void writeShortL(MemoryAccessor memoryAccessor, Object base, long offset, short v) {
        memoryAccessor.putByte(base, offset, (byte) ((v) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static int readInt(long address, boolean bigEndian) {
        return readInt(null, address, bigEndian);
    }

    public static int readInt(byte[] buffer, int pos, boolean bigEndian) {
        return readInt(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static int readInt(Object base, long offset, boolean bigEndian) {
        return readInt(MemoryAccessor.AMEM, base, offset, bigEndian);
    }

    public static int readInt(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readIntB(memoryAccessor, base, offset);
        } else {
            return readIntL(memoryAccessor, base, offset);
        }
    }

    public static int readIntB(Object base, long offset) {
        return readIntB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static int readIntL(Object base, long offset) {
        return readIntL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static int readIntB(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte3 = (memoryAccessor.getByte(base, offset) & 0xFF) << 24;
        int byte2 = (memoryAccessor.getByte(base, offset + 1) & 0xFF) << 16;
        int byte1 = (memoryAccessor.getByte(base, offset + 2) & 0xFF) << 8;
        int byte0 = memoryAccessor.getByte(base, offset + 3) & 0xFF;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static int readIntL(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte3 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte2 = (memoryAccessor.getByte(base, offset + 1) & 0xFF) << 8;
        int byte1 = (memoryAccessor.getByte(base, offset + 2) & 0xFF) << 16;
        int byte0 = (memoryAccessor.getByte(base, offset + 3) & 0xFF) << 24;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static void writeInt(long address, int v, boolean bigEndian) {
        writeInt(null, address, v, bigEndian);
    }

    public static void writeInt(byte[] buffer, int pos, int v, boolean bigEndian) {
        writeInt(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeInt(Object base, long offset, int v, boolean bigEndian) {
        writeInt(MemoryAccessor.AMEM, base, offset, v, bigEndian);
    }

    public static void writeInt(MemoryAccessor memoryAccessor, Object base, long offset, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntB(memoryAccessor, base, offset, v);
        } else {
            writeIntL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeIntB(Object base, long offset, int v) {
        writeIntB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeIntL(Object base, long offset, int v) {
        writeIntL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeIntB(MemoryAccessor memoryAccessor, Object base, long offset, int v) {
        memoryAccessor.putByte(base, offset, (byte) ((v >>> 24) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v >>> 16) & 0xFF));
        memoryAccessor.putByte(base, offset + 2, (byte) ((v >>> 8) & 0xFF));
        memoryAccessor.putByte(base, offset + 3, (byte) ((v) & 0xFF));
    }

    public static void writeIntL(MemoryAccessor memoryAccessor, Object base, long offset, int v) {
        memoryAccessor.putByte(base, offset, (byte) ((v) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
        memoryAccessor.putByte(base, offset + 2, (byte) ((v >>> 16) & 0xFF));
        memoryAccessor.putByte(base, offset + 3, (byte) ((v >>> 24) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static float readFloat(long address, boolean bigEndian) {
        return readFloat(null, address, bigEndian);
    }

    public static float readFloat(byte[] buffer, int pos, boolean bigEndian) {
        return readFloat(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static float readFloat(Object base, long offset, boolean bigEndian) {
        return readFloat(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static float readFloat(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readFloatB(memoryAccessor, base, offset);
        } else {
            return readFloatL(memoryAccessor, base, offset);
        }
    }

    public static float readFloatB(Object base, long offset) {
        return readFloatB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static float readFloatL(Object base, long offset) {
        return readFloatL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static float readFloatB(MemoryAccessor memoryAccessor, Object base, long offset) {
        return Float.intBitsToFloat(readIntB(memoryAccessor, base, offset));
    }

    public static float readFloatL(MemoryAccessor memoryAccessor, Object base, long offset) {
        return Float.intBitsToFloat(readIntL(memoryAccessor, base, offset));
    }

    public static void writeFloat(long address, float v, boolean bigEndian) {
        writeFloat(null, address, v, bigEndian);
    }

    public static void writeFloat(byte[] buffer, int pos, float v, boolean bigEndian) {
        writeFloat(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeFloat(Object base, long offset, float v, boolean bigEndian) {
        writeFloat(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeFloat(MemoryAccessor memoryAccessor, Object base, long offset, float v, boolean bigEndian) {
        if (bigEndian) {
            writeFloatB(memoryAccessor, base, offset, v);
        } else {
            writeFloatL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeFloatB(Object base, long offset, float v) {
        writeFloatB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeFloatL(Object base, long offset, float v) {
        writeFloatL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeFloatB(MemoryAccessor memoryAccessor, Object base, long offset, float v) {
        writeIntB(memoryAccessor, base, offset, Float.floatToRawIntBits(v));
    }

    public static void writeFloatL(MemoryAccessor memoryAccessor, Object base, long offset, float v) {
        writeIntL(memoryAccessor, base, offset, Float.floatToRawIntBits(v));
    }

    //////////////////////////////////////////////////////////////////

    public static long readLong(long address, boolean bigEndian) {
        return readLong(null, address, bigEndian);
    }

    public static long readLong(byte[] buffer, int pos, boolean bigEndian) {
        return readLong(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static long readLong(Object base, long offset, boolean bigEndian) {
        return readLong(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static long readLong(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readLongB(memoryAccessor, base, offset);
        } else {
            return readLongL(memoryAccessor, base, offset);
        }
    }

    public static long readLongB(Object base, long offset) {
        return readLongB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static long readLongL(Object base, long offset) {
        return readLongL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static long readLongB(MemoryAccessor memoryAccessor, Object base, long offset) {
        long byte7 = (long) memoryAccessor.getByte(base, offset) << 56;
        long byte6 = (long) (memoryAccessor.getByte(base, offset + 1) & 0xFF) << 48;
        long byte5 = (long) (memoryAccessor.getByte(base, offset + 2) & 0xFF) << 40;
        long byte4 = (long) (memoryAccessor.getByte(base, offset + 3) & 0xFF) << 32;
        long byte3 = (long) (memoryAccessor.getByte(base, offset + 4) & 0xFF) << 24;
        long byte2 = (long) (memoryAccessor.getByte(base, offset + 5) & 0xFF) << 16;
        long byte1 = (long) (memoryAccessor.getByte(base, offset + 6) & 0xFF) << 8;
        long byte0 = (long) (memoryAccessor.getByte(base, offset + 7) & 0xFF);
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static long readLongL(MemoryAccessor memoryAccessor, Object base, long offset) {
        long byte7 = (long) (memoryAccessor.getByte(base, offset) & 0xFF);
        long byte6 = (long) (memoryAccessor.getByte(base, offset + 1) & 0xFF) << 8;
        long byte5 = (long) (memoryAccessor.getByte(base, offset + 2) & 0xFF) << 16;
        long byte4 = (long) (memoryAccessor.getByte(base, offset + 3) & 0xFF) << 24;
        long byte3 = (long) (memoryAccessor.getByte(base, offset + 4) & 0xFF) << 32;
        long byte2 = (long) (memoryAccessor.getByte(base, offset + 5) & 0xFF) << 40;
        long byte1 = (long) (memoryAccessor.getByte(base, offset + 6) & 0xFF) << 48;
        long byte0 = (long) (memoryAccessor.getByte(base, offset + 7) & 0xFF) << 56;
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    //////////////////////////////////////////////////////////////////////////////

    public static void writeLong(long address, long v, boolean bigEndian) {
        writeLong(null, address, v, bigEndian);
    }

    public static void writeLong(byte[] buffer, int pos, long v, boolean bigEndian) {
        writeLong(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeLong(Object base, long offset, long v, boolean bigEndian) {
        writeLong(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeLong(MemoryAccessor memoryAccessor, Object base, long offset, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongB(memoryAccessor, base, offset, v);
        } else {
            writeLongL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeLongB(Object base, long offset, long v) {
        writeLongB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeLongL(Object base, long offset, long v) {
        writeLongL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeLongB(MemoryAccessor memoryAccessor, Object base, long offset, long v) {
        memoryAccessor.putByte(base, offset, (byte) (v >>> 56));
        memoryAccessor.putByte(base, offset + 1, (byte) (v >>> 48));
        memoryAccessor.putByte(base, offset + 2, (byte) (v >>> 40));
        memoryAccessor.putByte(base, offset + 3, (byte) (v >>> 32));
        memoryAccessor.putByte(base, offset + 4, (byte) (v >>> 24));
        memoryAccessor.putByte(base, offset + 5, (byte) (v >>> 16));
        memoryAccessor.putByte(base, offset + 6, (byte) (v >>> 8));
        memoryAccessor.putByte(base, offset + 7, (byte) (v));
    }

    public static void writeLongL(MemoryAccessor memoryAccessor, Object base, long offset, long v) {
        memoryAccessor.putByte(base, offset, (byte) (v));
        memoryAccessor.putByte(base, offset + 1, (byte) (v >>> 8));
        memoryAccessor.putByte(base, offset + 2, (byte) (v >>> 16));
        memoryAccessor.putByte(base, offset + 3, (byte) (v >>> 24));
        memoryAccessor.putByte(base, offset + 4, (byte) (v >>> 32));
        memoryAccessor.putByte(base, offset + 5, (byte) (v >>> 40));
        memoryAccessor.putByte(base, offset + 6, (byte) (v >>> 48));
        memoryAccessor.putByte(base, offset + 7, (byte) (v >>> 56));
    }

    //////////////////////////////////////////////////////////////////

    public static double readDouble(long address, boolean bigEndian) {
        return readDouble(null, address, bigEndian);
    }

    public static double readDouble(byte[] buffer, int pos, boolean bigEndian) {
        return readDouble(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static double readDouble(Object base, long offset, boolean bigEndian) {
        return readDouble(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static double readDouble(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readDoubleB(memoryAccessor, base, offset);
        } else {
            return readDoubleL(memoryAccessor, base, offset);
        }
    }

    public static double readDoubleB(Object base, long offset) {
        return readDoubleB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static double readDoubleL(Object base, long offset) {
        return readDoubleL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static double readDoubleB(MemoryAccessor memoryAccessor, Object base, long offset) {
        return Double.longBitsToDouble(readLongB(memoryAccessor, base, offset));
    }

    public static double readDoubleL(MemoryAccessor memoryAccessor, Object base, long offset) {
        return Double.longBitsToDouble(readLongL(memoryAccessor, base, offset));
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static void writeDouble(long address, double v, boolean bigEndian) {
        writeDouble(null, address, v, bigEndian);
    }

    public static void writeDouble(byte[] buffer, int pos, double v, boolean bigEndian) {
        writeDouble(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeDouble(Object base, long offset, double v, boolean bigEndian) {
        writeDouble(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeDouble(MemoryAccessor memoryAccessor, Object base, long offset, double v, boolean bigEndian) {
        if (bigEndian) {
            writeDoubleB(memoryAccessor, base, offset, v);
        } else {
            writeDoubleL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeDoubleB(Object base, long offset, double v) {
        writeLongB(STANDARD_MEMORY_ACCESSOR, base, offset, Double.doubleToRawLongBits(v));
    }

    public static void writeDoubleL(Object base, long offset, double v) {
        writeLongL(STANDARD_MEMORY_ACCESSOR, base, offset, Double.doubleToRawLongBits(v));
    }

    public static void writeDoubleB(MemoryAccessor memoryAccessor, Object base, long offset, double v) {
        writeLongB(memoryAccessor, base, offset, Double.doubleToRawLongBits(v));
    }

    public static void writeDoubleL(MemoryAccessor memoryAccessor, Object base, long offset, double v) {
        writeLongL(memoryAccessor, base, offset, Double.doubleToRawLongBits(v));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static char readCharVolatile(long address, boolean bigEndian) {
        return readCharVolatile(null, address, bigEndian);
    }

    public static char readCharVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readCharVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static char readCharVolatile(Object base, long offset, boolean bigEndian) {
        return readCharVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static char readCharVolatile(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readCharVolatileB(memoryAccessor, base, offset);
        } else {
            return readCharVolatileL(memoryAccessor, base, offset);
        }
    }

    public static char readCharVolatileB(Object base, long offset) {
        return readCharVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static char readCharVolatileL(Object base, long offset) {
        return readCharVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static char readCharVolatileB(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte1 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte0 = memoryAccessor.getByte(base, offset + 1) & 0xFF;
        return (char) ((byte1 << 8) + byte0);
    }

    public static char readCharVolatileL(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte1 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte0 = memoryAccessor.getByte(base, offset + 1) & 0xFF;
        return (char) ((byte0 << 8) + byte1);
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static void writeCharVolatile(long address, char v, boolean bigEndian) {
        writeCharVolatile(null, address, v, bigEndian);
    }

    public static void writeCharVolatile(byte[] buffer, int pos, char v, boolean bigEndian) {
        writeCharVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeCharVolatile(Object base, long offset, char v, boolean bigEndian) {
        writeCharVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeCharVolatile(MemoryAccessor memoryAccessor, Object base, long offset, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharVolatileB(memoryAccessor, base, offset, v);
        } else {
            writeCharVolatileL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeCharVolatileB(Object base, long offset, char v) {
        writeCharVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeCharVolatileL(Object base, long offset, char v) {
        writeCharVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeCharVolatileB(MemoryAccessor memoryAccessor, Object base, long offset, char v) {
        memoryAccessor.putByte(base, offset, (byte) ((v >>> 8) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v) & 0xFF));
    }

    public static void writeCharVolatileL(MemoryAccessor memoryAccessor, Object base, long offset, char v) {
        memoryAccessor.putByte(base, offset, (byte) ((v) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static short readShortVolatile(long address, boolean bigEndian) {
        return readShortVolatile(null, address, bigEndian);
    }

    public static short readShortVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readShortVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static short readShortVolatile(Object base, long offset, boolean bigEndian) {
        return readShortVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static short readShortVolatile(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readShortVolatileB(memoryAccessor, base, offset);
        } else {
            return readShortVolatileL(memoryAccessor, base, offset);
        }
    }

    public static short readShortVolatileB(Object base, long offset) {
        return readShortVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static short readShortVolatileL(Object base, long offset) {
        return readShortVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static short readShortVolatileB(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte1 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte0 = memoryAccessor.getByte(base, offset + 1) & 0xFF;
        return (short) ((byte1 << 8) + byte0);
    }

    public static short readShortVolatileL(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte1 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte0 = memoryAccessor.getByte(base, offset + 1) & 0xFF;
        return (short) ((byte0 << 8) + byte1);
    }

    //////////////////////////////////////////////////////////////////

    public static void writeShortVolatile(long address, short v, boolean bigEndian) {
        writeShortVolatile(null, address, v, bigEndian);
    }

    public static void writeShortVolatile(byte[] buffer, int pos, short v, boolean bigEndian) {
        writeShortVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeShortVolatile(Object base, long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortVolatileB(base, offset, v);
        } else {
            writeShortVolatileL(base, offset, v);
        }
    }

    public static void writeShortVolatile(MemoryAccessor memoryAccessor, Object base, long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortVolatileB(memoryAccessor, base, offset, v);
        } else {
            writeShortVolatileL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeShortVolatileB(Object base, long offset, short v) {
        writeShortVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeShortVolatileL(Object base, long offset, short v) {
        writeShortVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeShortVolatileB(MemoryAccessor memoryAccessor, Object base, long offset, short v) {
        memoryAccessor.putByte(base, offset, (byte) ((v >>> 8) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v) & 0xFF));
    }

    public static void writeShortVolatileL(MemoryAccessor memoryAccessor, Object base, long offset, short v) {
        memoryAccessor.putByte(base, offset, (byte) ((v) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static int readIntVolatile(long address, boolean bigEndian) {
        return readIntVolatile(null, address, bigEndian);
    }

    public static int readIntVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readIntVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static int readIntVolatile(Object base, long offset, boolean bigEndian) {
        return readIntVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static int readIntVolatile(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readIntVolatileB(memoryAccessor, base, offset);
        } else {
            return readIntVolatileL(memoryAccessor, base, offset);
        }
    }

    public static int readIntVolatileB(Object base, long offset) {
        return readIntVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static int readIntVolatileL(Object base, long offset) {
        return readIntVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static int readIntVolatileB(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte3 = (memoryAccessor.getByte(base, offset) & 0xFF) << 24;
        int byte2 = (memoryAccessor.getByte(base, offset + 1) & 0xFF) << 16;
        int byte1 = (memoryAccessor.getByte(base, offset + 2) & 0xFF) << 8;
        int byte0 = memoryAccessor.getByte(base, offset + 3) & 0xFF;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static int readIntVolatileL(MemoryAccessor memoryAccessor, Object base, long offset) {
        int byte3 = memoryAccessor.getByte(base, offset) & 0xFF;
        int byte2 = (memoryAccessor.getByte(base, offset + 1) & 0xFF) << 8;
        int byte1 = (memoryAccessor.getByte(base, offset + 2) & 0xFF) << 16;
        int byte0 = (memoryAccessor.getByte(base, offset + 3) & 0xFF) << 24;
        return byte3 + byte2 + byte1 + byte0;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    public static void writeIntVolatile(long address, int v, boolean bigEndian) {
        writeIntVolatile(null, address, v, bigEndian);
    }

    public static void writeIntVolatile(byte[] buffer, int pos, int v, boolean bigEndian) {
        writeIntVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeIntVolatile(Object base, long offset, int v, boolean bigEndian) {
        writeIntVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeIntVolatile(MemoryAccessor memoryAccessor, Object base, long offset, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntVolatileB(memoryAccessor, base, offset, v);
        } else {
            writeIntVolatileL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeIntVolatileB(Object base, long offset, int v) {
        writeIntVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeIntVolatileL(Object base, long offset, int v) {
        writeIntVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeIntVolatileB(MemoryAccessor memoryAccessor, Object base, long offset, int v) {
        memoryAccessor.putByte(base, offset, (byte) ((v >>> 24) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v >>> 16) & 0xFF));
        memoryAccessor.putByte(base, offset + 2, (byte) ((v >>> 8) & 0xFF));
        memoryAccessor.putByte(base, offset + 3, (byte) ((v) & 0xFF));
    }

    public static void writeIntVolatileL(MemoryAccessor memoryAccessor, Object base, long offset, int v) {
        memoryAccessor.putByte(base, offset, (byte) ((v) & 0xFF));
        memoryAccessor.putByte(base, offset + 1, (byte) ((v >>> 8) & 0xFF));
        memoryAccessor.putByte(base, offset + 2, (byte) ((v >>> 16) & 0xFF));
        memoryAccessor.putByte(base, offset + 3, (byte) ((v >>> 24) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static float readFloatVolatile(long address, boolean bigEndian) {
        return readFloatVolatile(null, address, bigEndian);
    }

    public static float readFloatVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readFloatVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static float readFloatVolatile(Object base, long offset, boolean bigEndian) {
        return readFloatVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static float readFloatVolatile(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readFloatVolatileB(memoryAccessor, base, offset);
        } else {
            return readFloatVolatileL(memoryAccessor, base, offset);
        }
    }

    public static float readFloatVolatileB(Object base, long offset) {
        return readFloatVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static float readFloatVolatileL(Object base, long offset) {
        return readFloatVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static float readFloatVolatileB(MemoryAccessor memoryAccessor, Object base, long offset) {
        return Float.intBitsToFloat(readIntVolatileB(memoryAccessor, base, offset));
    }

    public static float readFloatVolatileL(MemoryAccessor memoryAccessor, Object base, long offset) {
        return Float.intBitsToFloat(readIntVolatileL(memoryAccessor, base, offset));
    }

    //////////////////////////////////////////////////////////////////

    public static void writeFloatVolatile(long address, float v, boolean bigEndian) {
        writeFloatVolatile(null, address, v, bigEndian);
    }

    public static void writeFloatVolatile(byte[] buffer, int pos, float v, boolean bigEndian) {
        writeFloatVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeFloatVolatile(Object base, long offset, float v, boolean bigEndian) {
        writeFloatVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeFloatVolatile(MemoryAccessor memoryAccessor, Object base, long offset, float v, boolean bigEndian) {
        if (bigEndian) {
            writeFloatVolatileB(memoryAccessor, base, offset, v);
        } else {
            writeFloatVolatileL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeFloatVolatileB(Object base, long offset, float v) {
        writeFloatVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset, Float.floatToRawIntBits(v));
    }

    public static void writeFloatVolatileL(Object base, long offset, float v) {
        writeFloatVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset, Float.floatToRawIntBits(v));
    }

    public static void writeFloatVolatileB(MemoryAccessor memoryAccessor, Object base, long offset, float v) {
        writeIntVolatileB(memoryAccessor, base, offset, Float.floatToRawIntBits(v));
    }

    public static void writeFloatVolatileL(MemoryAccessor memoryAccessor, Object base, long offset, float v) {
        writeIntVolatileL(memoryAccessor, base, offset, Float.floatToRawIntBits(v));
    }

    //////////////////////////////////////////////////////////////////

    public static long readLongVolatile(long address, boolean bigEndian) {
        return readLongVolatile(null, address, bigEndian);
    }

    public static long readLongVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readLongVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static long readLongVolatile(Object base, long offset, boolean bigEndian) {
        return readLongVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static long readLongVolatile(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readLongVolatileB(memoryAccessor, base, offset);
        } else {
            return readLongVolatileL(memoryAccessor, base, offset);
        }
    }

    public static long readLongVolatileB(Object base, long offset) {
        return readLongVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static long readLongVolatileL(Object base, long offset) {
        return readLongVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static long readLongVolatileB(MemoryAccessor memoryAccessor, Object base, long offset) {
        long byte7 = (long) memoryAccessor.getByte(base, offset) << 56;
        long byte6 = (long) (memoryAccessor.getByte(base, offset + 1) & 0xFF) << 48;
        long byte5 = (long) (memoryAccessor.getByte(base, offset + 2) & 0xFF) << 40;
        long byte4 = (long) (memoryAccessor.getByte(base, offset + 3) & 0xFF) << 32;
        long byte3 = (long) (memoryAccessor.getByte(base, offset + 4) & 0xFF) << 24;
        long byte2 = (long) (memoryAccessor.getByte(base, offset + 5) & 0xFF) << 16;
        long byte1 = (long) (memoryAccessor.getByte(base, offset + 6) & 0xFF) << 8;
        long byte0 = (long) (memoryAccessor.getByte(base, offset + 7) & 0xFF);
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static long readLongVolatileL(MemoryAccessor memoryAccessor, Object base, long offset) {
        long byte7 = (long) (memoryAccessor.getByte(base, offset) & 0xFF);
        long byte6 = (long) (memoryAccessor.getByte(base, offset + 1) & 0xFF) << 8;
        long byte5 = (long) (memoryAccessor.getByte(base, offset + 2) & 0xFF) << 16;
        long byte4 = (long) (memoryAccessor.getByte(base, offset + 3) & 0xFF) << 24;
        long byte3 = (long) (memoryAccessor.getByte(base, offset + 4) & 0xFF) << 32;
        long byte2 = (long) (memoryAccessor.getByte(base, offset + 5) & 0xFF) << 40;
        long byte1 = (long) (memoryAccessor.getByte(base, offset + 6) & 0xFF) << 48;
        long byte0 = (long) (memoryAccessor.getByte(base, offset + 7) & 0xFF) << 56;
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void writeLongVolatile(long address, long v, boolean bigEndian) {
        writeLongVolatile(null, address, v, bigEndian);
    }

    public static void writeLongVolatile(byte[] buffer, int pos, long v, boolean bigEndian) {
        writeLongVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeLongVolatile(Object base, long offset, long v, boolean bigEndian) {
        writeLongVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeLongVolatile(MemoryAccessor memoryAccessor, Object base, long offset, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongVolatileB(memoryAccessor, base, offset, v);
        } else {
            writeLongVolatileL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeLongVolatileB(Object base, long offset, long v) {
        writeLongVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeLongVolatileL(Object base, long offset, long v) {
        writeLongVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeLongVolatileB(MemoryAccessor memoryAccessor, Object base, long offset, long v) {
        memoryAccessor.putByte(base, offset, (byte) (v >>> 56));
        memoryAccessor.putByte(base, offset + 1, (byte) (v >>> 48));
        memoryAccessor.putByte(base, offset + 2, (byte) (v >>> 40));
        memoryAccessor.putByte(base, offset + 3, (byte) (v >>> 32));
        memoryAccessor.putByte(base, offset + 4, (byte) (v >>> 24));
        memoryAccessor.putByte(base, offset + 5, (byte) (v >>> 16));
        memoryAccessor.putByte(base, offset + 6, (byte) (v >>> 8));
        memoryAccessor.putByte(base, offset + 7, (byte) (v));
    }

    public static void writeLongVolatileL(MemoryAccessor memoryAccessor, Object base, long offset, long v) {
        memoryAccessor.putByte(base, offset, (byte) (v));
        memoryAccessor.putByte(base, offset + 1, (byte) (v >>> 8));
        memoryAccessor.putByte(base, offset + 2, (byte) (v >>> 16));
        memoryAccessor.putByte(base, offset + 3, (byte) (v >>> 24));
        memoryAccessor.putByte(base, offset + 4, (byte) (v >>> 32));
        memoryAccessor.putByte(base, offset + 5, (byte) (v >>> 40));
        memoryAccessor.putByte(base, offset + 6, (byte) (v >>> 48));
        memoryAccessor.putByte(base, offset + 7, (byte) (v >>> 56));
    }

    //////////////////////////////////////////////////////////////////

    public static double readDoubleVolatile(long address, boolean bigEndian) {
        return readDoubleVolatile(null, address, bigEndian);
    }

    public static double readDoubleVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readDoubleVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static double readDoubleVolatile(Object base, long offset, boolean bigEndian) {
        return readDoubleVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, bigEndian);
    }

    public static double readDoubleVolatile(MemoryAccessor memoryAccessor, Object base, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readDoubleVolatileB(memoryAccessor, base, offset);
        } else {
            return readDoubleVolatileL(memoryAccessor, base, offset);
        }
    }

    public static double readDoubleVolatileB(Object base, long offset) {
        return readDoubleVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static double readDoubleVolatileL(Object base, long offset) {
        return readDoubleVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset);
    }

    public static double readDoubleVolatileB(MemoryAccessor memoryAccessor, Object base, long offset) {
        return Double.longBitsToDouble(readLongVolatileB(memoryAccessor, base, offset));
    }

    public static double readDoubleVolatileL(MemoryAccessor memoryAccessor, Object base, long offset) {
        return Double.longBitsToDouble(readLongVolatileL(memoryAccessor, base, offset));
    }

    //////////////////////////////////////////////////////////////////

    public static void writeDoubleVolatile(long address, double v, boolean bigEndian) {
        writeDoubleVolatile(null, address, v, bigEndian);
    }

    public static void writeDoubleVolatile(byte[] buffer, int pos, double v, boolean bigEndian) {
        writeDoubleVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeDoubleVolatile(Object base, long offset, double v, boolean bigEndian) {
        writeDoubleVolatile(STANDARD_MEMORY_ACCESSOR, base, offset, v, bigEndian);
    }

    public static void writeDoubleVolatile(MemoryAccessor memoryAccessor, Object base, long offset, double v, boolean bigEndian) {
        if (bigEndian) {
            writeDoubleVolatileB(memoryAccessor, base, offset, v);
        } else {
            writeDoubleVolatileL(memoryAccessor, base, offset, v);
        }
    }

    public static void writeDoubleVolatileB(Object base, long offset, double v) {
        writeDoubleVolatileB(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeDoubleVolatileL(Object base, long offset, double v) {
        writeDoubleVolatileL(STANDARD_MEMORY_ACCESSOR, base, offset, v);
    }

    public static void writeDoubleVolatileB(MemoryAccessor memoryAccessor, Object base, long offset, double v) {
        writeLongVolatileB(memoryAccessor, base, offset, Double.doubleToRawLongBits(v));
    }

    public static void writeDoubleVolatileL(MemoryAccessor memoryAccessor, Object base, long offset, double v) {
        writeLongVolatileL(memoryAccessor, base, offset, Double.doubleToRawLongBits(v));
    }


    //////////////////////////////////////////////////////////////////

    public static int writeUtf8Char(byte[] buffer, int pos, int c) {
        return writeUtf8Char(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, c);
    }

    public static int writeUtf8Char(long bufferPointer, long pos, int c) {
        return writeUtf8Char(STANDARD_MEMORY_ACCESSOR, null, bufferPointer + pos, c);
    }

    public static int writeUtf8Char(MemoryAccessor memoryAccessor, Object base, long pos, int c) {
        if (c <= 0x007F) {
            memoryAccessor.putByte(base, pos, (byte) c);
            return 1;
        } else if (c > 0x07FF) {
            memoryAccessor.putByte(base, pos, (byte) (0xE0 | c >> 12 & 0x0F));
            memoryAccessor.putByte(base, pos + 1, (byte) (0x80 | c >> 6 & 0x3F));
            memoryAccessor.putByte(base, pos + 2, (byte) (0x80 | c & 0x3F));
            return 3;
        } else {
            memoryAccessor.putByte(base, pos, (byte) (0xC0 | c >> 6 & 0x1F));
            memoryAccessor.putByte(base, pos + 1, (byte) (0x80 | c & 0x3F));
            return 2;
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
}
