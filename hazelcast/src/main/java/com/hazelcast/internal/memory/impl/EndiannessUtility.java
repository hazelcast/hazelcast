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

import com.hazelcast.internal.memory.ByteMemoryAccessStrategy;
import com.hazelcast.internal.memory.MemoryAccessStrategy;

import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;

/**
 * Utility class to read/write bits to given location (by base object/offset or native memory address)
 * by specified byte order (little/big endian).
 */
@SuppressWarnings({"checkstyle:magicnumber", "checkstyle:methodcount" })
public final class EndiannessUtility {
    private static final ByteMemoryAccessStrategy BYTE_ARRAY_MEMORY_ACCESSOR = MemoryAccessStrategy.HEAP_BYTE_ARRAY_MEM;
    private static final ByteMemoryAccessStrategy OFF_HEAP_MEMORY_ACCESSOR = MemoryAccessStrategy.STANDARD_MEM;

    private EndiannessUtility() {
    }

    //////////////////////////////////////////////////////////////////

    public static char readChar(long address, boolean bigEndian) {
        return readChar(null, address, bigEndian);
    }

    public static char readChar(byte[] buffer, int pos, boolean bigEndian) {
        return readChar(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static char readChar(Object resource, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readCharB(resource, offset);
        } else {
            return readCharL(resource, offset);
        }
    }

    public static <R> char readChar(ByteMemoryAccessStrategy<R> memoryAccessStrategy,
                                    R resource, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readCharB(memoryAccessStrategy, resource, offset);
        } else {
            return readCharL(memoryAccessStrategy, resource, offset);
        }
    }

    public static char readCharB(Object resource, long offset) {
        return readCharB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static char readCharL(Object resource, long offset) {
        return readCharL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> char readCharB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte1 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF;
        return (char) ((byte1 << 8) + byte0);
    }

    public static <R> char readCharL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte1 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF;
        return (char) ((byte0 << 8) + byte1);
    }

    public static void writeChar(long address, char v, boolean bigEndian) {
        writeChar(null, address, v, bigEndian);
    }

    public static void writeChar(byte[] buffer, int pos, char v, boolean bigEndian) {
        writeChar(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeChar(Object resource, long offset, char v, boolean bigEndian) {
        writeChar(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeChar(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                     long offset, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeCharL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static <R> void writeCharB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, char v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v >>> 8) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v) & 0xFF));
    }

    public static <R> void writeCharL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, char v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    public static void writeCharB(Object resource, long offset, char v) {
        writeCharB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeCharL(Object resource, long offset, char v) {
        writeCharL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    //////////////////////////////////////////////////////////////////

    public static short readShort(long address, boolean bigEndian) {
        return readShort(null, address, bigEndian);
    }

    public static short readShort(byte[] buffer, int pos, boolean bigEndian) {
        return readShort(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static short readShort(Object resource, long offset, boolean bigEndian) {
        return readShort(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> short readShort(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                      long offset, boolean bigEndian) {
        if (bigEndian) {
            return readShortB(memoryAccessStrategy, resource, offset);
        } else {
            return readShortL(memoryAccessStrategy, resource, offset);
        }
    }

    public static short readShortB(Object resource, long offset) {
        return readShortB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static short readShortL(Object resource, long offset) {
        return readShortL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> short readShortB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte1 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF;
        return (short) ((byte1 << 8) + byte0);
    }

    public static <R> short readShortL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte1 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF;
        return (short) ((byte0 << 8) + byte1);
    }

    public static void writeShort(long address, short v, boolean bigEndian) {
        writeShort(null, address, v, bigEndian);
    }

    public static void writeShort(byte[] buffer, int pos, short v, boolean bigEndian) {
        writeShort(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeShort(Object resource, long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortB(resource, offset, v);
        } else {
            writeShortL(resource, offset, v);
        }
    }

    public static <R> void writeShort(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                      long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeShortL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeShortB(Object resource, long offset, short v) {
        writeShortB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeShortL(Object resource, long offset, short v) {
        writeShortL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeShortB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                       long offset, short v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v >>> 8) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v) & 0xFF));
    }

    public static <R> void writeShortL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                       long offset, short v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static int readInt(long address, boolean bigEndian) {
        return readInt(null, address, bigEndian);
    }

    public static int readInt(byte[] buffer, int pos, boolean bigEndian) {
        return readInt(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static int readInt(Object resource, long offset, boolean bigEndian) {
        return readInt(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> int readInt(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, boolean bigEndian) {
        if (bigEndian) {
            return readIntB(memoryAccessStrategy, resource, offset);
        } else {
            return readIntL(memoryAccessStrategy, resource, offset);
        }
    }

    public static int readIntB(Object resource, long offset) {
        return readIntB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static int readIntL(Object resource, long offset) {
        return readIntL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> int readIntB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte3 = (memoryAccessStrategy.getByte(resource, offset) & 0xFF) << 24;
        int byte2 = (memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF) << 16;
        int byte1 = (memoryAccessStrategy.getByte(resource, offset + 2) & 0xFF) << 8;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 3) & 0xFF;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static <R> int readIntL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte3 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte2 = (memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF) << 8;
        int byte1 = (memoryAccessStrategy.getByte(resource, offset + 2) & 0xFF) << 16;
        int byte0 = (memoryAccessStrategy.getByte(resource, offset + 3) & 0xFF) << 24;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static void writeInt(long address, int v, boolean bigEndian) {
        writeInt(null, address, v, bigEndian);
    }

    public static void writeInt(byte[] buffer, int pos, int v, boolean bigEndian) {
        writeInt(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeInt(Object resource, long offset, int v, boolean bigEndian) {
        writeInt(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeInt(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                    long offset, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeIntL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeIntB(Object resource, long offset, int v) {
        writeIntB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeIntL(Object resource, long offset, int v) {
        writeIntL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeIntB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, int v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v >>> 24) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v >>> 16) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 2, (byte) ((v >>> 8) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 3, (byte) ((v) & 0xFF));
    }

    public static <R> void writeIntL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, int v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 2, (byte) ((v >>> 16) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 3, (byte) ((v >>> 24) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static float readFloat(long address, boolean bigEndian) {
        return readFloat(null, address, bigEndian);
    }

    public static float readFloat(byte[] buffer, int pos, boolean bigEndian) {
        return readFloat(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static float readFloat(Object resource, long offset, boolean bigEndian) {
        return readFloat(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> float readFloat(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                      long offset, boolean bigEndian) {
        if (bigEndian) {
            return readFloatB(memoryAccessStrategy, resource, offset);
        } else {
            return readFloatL(memoryAccessStrategy, resource, offset);
        }
    }

    public static float readFloatB(Object resource, long offset) {
        return readFloatB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static float readFloatL(Object resource, long offset) {
        return readFloatL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> float readFloatB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        return Float.intBitsToFloat(readIntB(memoryAccessStrategy, resource, offset));
    }

    public static <R> float readFloatL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        return Float.intBitsToFloat(readIntL(memoryAccessStrategy, resource, offset));
    }

    public static void writeFloat(long address, float v, boolean bigEndian) {
        writeFloat(null, address, v, bigEndian);
    }

    public static void writeFloat(byte[] buffer, int pos, float v, boolean bigEndian) {
        writeFloat(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeFloat(Object resource, long offset, float v, boolean bigEndian) {
        writeFloat(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeFloat(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                      long offset, float v, boolean bigEndian) {
        if (bigEndian) {
            writeFloatB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeFloatL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeFloatB(Object resource, long offset, float v) {
        writeFloatB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeFloatL(Object resource, long offset, float v) {
        writeFloatL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeFloatB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                       long offset, float v) {
        writeIntB(memoryAccessStrategy, resource, offset, Float.floatToRawIntBits(v));
    }

    public static <R> void writeFloatL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                       long offset, float v) {
        writeIntL(memoryAccessStrategy, resource, offset, Float.floatToRawIntBits(v));
    }

    //////////////////////////////////////////////////////////////////

    public static long readLong(long address, boolean bigEndian) {
        return readLong(null, address, bigEndian);
    }

    public static long readLong(byte[] buffer, int pos, boolean bigEndian) {
        return readLong(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static long readLong(Object resource, long offset, boolean bigEndian) {
        return readLong(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> long readLong(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                    long offset, boolean bigEndian) {
        if (bigEndian) {
            return readLongB(memoryAccessStrategy, resource, offset);
        } else {
            return readLongL(memoryAccessStrategy, resource, offset);
        }
    }

    public static long readLongB(Object resource, long offset) {
        return readLongB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static long readLongL(Object resource, long offset) {
        return readLongL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> long readLongB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        long byte7 = (long) memoryAccessStrategy.getByte(resource, offset) << 56;
        long byte6 = (long) (memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF) << 48;
        long byte5 = (long) (memoryAccessStrategy.getByte(resource, offset + 2) & 0xFF) << 40;
        long byte4 = (long) (memoryAccessStrategy.getByte(resource, offset + 3) & 0xFF) << 32;
        long byte3 = (long) (memoryAccessStrategy.getByte(resource, offset + 4) & 0xFF) << 24;
        long byte2 = (long) (memoryAccessStrategy.getByte(resource, offset + 5) & 0xFF) << 16;
        long byte1 = (long) (memoryAccessStrategy.getByte(resource, offset + 6) & 0xFF) << 8;
        long byte0 = (long) (memoryAccessStrategy.getByte(resource, offset + 7) & 0xFF);
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static <R> long readLongL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        long byte7 = (long) (memoryAccessStrategy.getByte(resource, offset) & 0xFF);
        long byte6 = (long) (memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF) << 8;
        long byte5 = (long) (memoryAccessStrategy.getByte(resource, offset + 2) & 0xFF) << 16;
        long byte4 = (long) (memoryAccessStrategy.getByte(resource, offset + 3) & 0xFF) << 24;
        long byte3 = (long) (memoryAccessStrategy.getByte(resource, offset + 4) & 0xFF) << 32;
        long byte2 = (long) (memoryAccessStrategy.getByte(resource, offset + 5) & 0xFF) << 40;
        long byte1 = (long) (memoryAccessStrategy.getByte(resource, offset + 6) & 0xFF) << 48;
        long byte0 = (long) (memoryAccessStrategy.getByte(resource, offset + 7) & 0xFF) << 56;
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    //////////////////////////////////////////////////////////////////////////////

    public static void writeLong(long address, long v, boolean bigEndian) {
        writeLong(null, address, v, bigEndian);
    }

    public static void writeLong(byte[] buffer, int pos, long v, boolean bigEndian) {
        writeLong(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeLong(Object resource, long offset, long v, boolean bigEndian) {
        writeLong(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeLong(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                     long offset, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeLongL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeLongB(Object resource, long offset, long v) {
        writeLongB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeLongL(Object resource, long offset, long v) {
        writeLongL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeLongB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, long v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) (v >>> 56));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) (v >>> 48));
        memoryAccessStrategy.putByte(resource, offset + 2, (byte) (v >>> 40));
        memoryAccessStrategy.putByte(resource, offset + 3, (byte) (v >>> 32));
        memoryAccessStrategy.putByte(resource, offset + 4, (byte) (v >>> 24));
        memoryAccessStrategy.putByte(resource, offset + 5, (byte) (v >>> 16));
        memoryAccessStrategy.putByte(resource, offset + 6, (byte) (v >>> 8));
        memoryAccessStrategy.putByte(resource, offset + 7, (byte) (v));
    }

    public static <R> void writeLongL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, long v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) (v));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) (v >>> 8));
        memoryAccessStrategy.putByte(resource, offset + 2, (byte) (v >>> 16));
        memoryAccessStrategy.putByte(resource, offset + 3, (byte) (v >>> 24));
        memoryAccessStrategy.putByte(resource, offset + 4, (byte) (v >>> 32));
        memoryAccessStrategy.putByte(resource, offset + 5, (byte) (v >>> 40));
        memoryAccessStrategy.putByte(resource, offset + 6, (byte) (v >>> 48));
        memoryAccessStrategy.putByte(resource, offset + 7, (byte) (v >>> 56));
    }

    //////////////////////////////////////////////////////////////////

    public static double readDouble(long address, boolean bigEndian) {
        return readDouble(null, address, bigEndian);
    }

    public static double readDouble(byte[] buffer, int pos, boolean bigEndian) {
        return readDouble(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static double readDouble(Object resource, long offset, boolean bigEndian) {
        return readDouble(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> double readDouble(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                        long offset, boolean bigEndian) {
        if (bigEndian) {
            return readDoubleB(memoryAccessStrategy, resource, offset);
        } else {
            return readDoubleL(memoryAccessStrategy, resource, offset);
        }
    }

    public static double readDoubleB(Object resource, long offset) {
        return readDoubleB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static double readDoubleL(Object resource, long offset) {
        return readDoubleL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> double readDoubleB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        return Double.longBitsToDouble(readLongB(memoryAccessStrategy, resource, offset));
    }

    public static <R> double readDoubleL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        return Double.longBitsToDouble(readLongL(memoryAccessStrategy, resource, offset));
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static void writeDouble(long address, double v, boolean bigEndian) {
        writeDouble(null, address, v, bigEndian);
    }

    public static void writeDouble(byte[] buffer, int pos, double v, boolean bigEndian) {
        writeDouble(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeDouble(Object resource, long offset, double v, boolean bigEndian) {
        writeDouble(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeDouble(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                       long offset, double v, boolean bigEndian) {
        if (bigEndian) {
            writeDoubleB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeDoubleL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeDoubleB(Object resource, long offset, double v) {
        writeLongB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, Double.doubleToRawLongBits(v));
    }

    public static void writeDoubleL(Object resource, long offset, double v) {
        writeLongL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, Double.doubleToRawLongBits(v));
    }

    public static <R> void writeDoubleB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, double v) {
        writeLongB(memoryAccessStrategy, resource, offset, Double.doubleToRawLongBits(v));
    }

    public static <R> void writeDoubleL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, double v) {
        writeLongL(memoryAccessStrategy, resource, offset, Double.doubleToRawLongBits(v));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static char readCharVolatile(long address, boolean bigEndian) {
        return readCharVolatile(null, address, bigEndian);
    }

    public static char readCharVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readCharVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static char readCharVolatile(Object resource, long offset, boolean bigEndian) {
        return readCharVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> char readCharVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                            long offset, boolean bigEndian) {
        if (bigEndian) {
            return readCharVolatileB(memoryAccessStrategy, resource, offset);
        } else {
            return readCharVolatileL(memoryAccessStrategy, resource, offset);
        }
    }

    public static char readCharVolatileB(Object resource, long offset) {
        return readCharVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static char readCharVolatileL(Object resource, long offset) {
        return readCharVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> char readCharVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte1 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF;
        return (char) ((byte1 << 8) + byte0);
    }

    public static <R> char readCharVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte1 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF;
        return (char) ((byte0 << 8) + byte1);
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static void writeCharVolatile(long address, char v, boolean bigEndian) {
        writeCharVolatile(null, address, v, bigEndian);
    }

    public static void writeCharVolatile(byte[] buffer, int pos, char v, boolean bigEndian) {
        writeCharVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeCharVolatile(Object resource, long offset, char v, boolean bigEndian) {
        writeCharVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeCharVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                             long offset, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharVolatileB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeCharVolatileL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeCharVolatileB(Object resource, long offset, char v) {
        writeCharVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeCharVolatileL(Object resource, long offset, char v) {
        writeCharVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeCharVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, char v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v >>> 8) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v) & 0xFF));
    }

    public static <R> void writeCharVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, char v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static short readShortVolatile(long address, boolean bigEndian) {
        return readShortVolatile(null, address, bigEndian);
    }

    public static short readShortVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readShortVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static short readShortVolatile(Object resource, long offset, boolean bigEndian) {
        return readShortVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> short readShortVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                              long offset, boolean bigEndian) {
        if (bigEndian) {
            return readShortVolatileB(memoryAccessStrategy, resource, offset);
        } else {
            return readShortVolatileL(memoryAccessStrategy, resource, offset);
        }
    }

    public static short readShortVolatileB(Object resource, long offset) {
        return readShortVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static short readShortVolatileL(Object resource, long offset) {
        return readShortVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> short readShortVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte1 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF;
        return (short) ((byte1 << 8) + byte0);
    }

    public static <R> short readShortVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte1 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF;
        return (short) ((byte0 << 8) + byte1);
    }

    //////////////////////////////////////////////////////////////////

    public static void writeShortVolatile(long address, short v, boolean bigEndian) {
        writeShortVolatile(null, address, v, bigEndian);
    }

    public static void writeShortVolatile(byte[] buffer, int pos, short v, boolean bigEndian) {
        writeShortVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeShortVolatile(Object resource, long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortVolatileB(resource, offset, v);
        } else {
            writeShortVolatileL(resource, offset, v);
        }
    }

    public static <R> void writeShortVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                              long offset, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortVolatileB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeShortVolatileL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeShortVolatileB(Object resource, long offset, short v) {
        writeShortVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeShortVolatileL(Object resource, long offset, short v) {
        writeShortVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeShortVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                               long offset, short v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v >>> 8) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v) & 0xFF));
    }

    public static <R> void writeShortVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                               long offset, short v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static int readIntVolatile(long address, boolean bigEndian) {
        return readIntVolatile(null, address, bigEndian);
    }

    public static int readIntVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readIntVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static int readIntVolatile(Object resource, long offset, boolean bigEndian) {
        return readIntVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> int readIntVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                          long offset, boolean bigEndian) {
        if (bigEndian) {
            return readIntVolatileB(memoryAccessStrategy, resource, offset);
        } else {
            return readIntVolatileL(memoryAccessStrategy, resource, offset);
        }
    }

    public static int readIntVolatileB(Object resource, long offset) {
        return readIntVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static int readIntVolatileL(Object resource, long offset) {
        return readIntVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> int readIntVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte3 = (memoryAccessStrategy.getByte(resource, offset) & 0xFF) << 24;
        int byte2 = (memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF) << 16;
        int byte1 = (memoryAccessStrategy.getByte(resource, offset + 2) & 0xFF) << 8;
        int byte0 = memoryAccessStrategy.getByte(resource, offset + 3) & 0xFF;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static <R> int readIntVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        int byte3 = memoryAccessStrategy.getByte(resource, offset) & 0xFF;
        int byte2 = (memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF) << 8;
        int byte1 = (memoryAccessStrategy.getByte(resource, offset + 2) & 0xFF) << 16;
        int byte0 = (memoryAccessStrategy.getByte(resource, offset + 3) & 0xFF) << 24;
        return byte3 + byte2 + byte1 + byte0;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    public static void writeIntVolatile(long address, int v, boolean bigEndian) {
        writeIntVolatile(null, address, v, bigEndian);
    }

    public static void writeIntVolatile(byte[] buffer, int pos, int v, boolean bigEndian) {
        writeIntVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeIntVolatile(Object resource, long offset, int v, boolean bigEndian) {
        writeIntVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeIntVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                            long offset, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntVolatileB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeIntVolatileL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeIntVolatileB(Object resource, long offset, int v) {
        writeIntVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeIntVolatileL(Object resource, long offset, int v) {
        writeIntVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeIntVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                             long offset, int v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v >>> 24) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v >>> 16) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 2, (byte) ((v >>> 8) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 3, (byte) ((v) & 0xFF));
    }

    public static <R> void writeIntVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                             long offset, int v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 2, (byte) ((v >>> 16) & 0xFF));
        memoryAccessStrategy.putByte(resource, offset + 3, (byte) ((v >>> 24) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////

    public static float readFloatVolatile(long address, boolean bigEndian) {
        return readFloatVolatile(null, address, bigEndian);
    }

    public static float readFloatVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readFloatVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static float readFloatVolatile(Object resource, long offset, boolean bigEndian) {
        return readFloatVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> float readFloatVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                              long offset, boolean bigEndian) {
        if (bigEndian) {
            return readFloatVolatileB(memoryAccessStrategy, resource, offset);
        } else {
            return readFloatVolatileL(memoryAccessStrategy, resource, offset);
        }
    }

    public static float readFloatVolatileB(Object resource, long offset) {
        return readFloatVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static float readFloatVolatileL(Object resource, long offset) {
        return readFloatVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> float readFloatVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        return Float.intBitsToFloat(readIntVolatileB(memoryAccessStrategy, resource, offset));
    }

    public static <R> float readFloatVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        return Float.intBitsToFloat(readIntVolatileL(memoryAccessStrategy, resource, offset));
    }

    //////////////////////////////////////////////////////////////////

    public static void writeFloatVolatile(long address, float v, boolean bigEndian) {
        writeFloatVolatile(null, address, v, bigEndian);
    }

    public static void writeFloatVolatile(byte[] buffer, int pos, float v, boolean bigEndian) {
        writeFloatVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeFloatVolatile(Object resource, long offset, float v, boolean bigEndian) {
        writeFloatVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeFloatVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                              long offset, float v, boolean bigEndian) {
        if (bigEndian) {
            writeFloatVolatileB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeFloatVolatileL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeFloatVolatileB(Object resource, long offset, float v) {
        writeFloatVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, Float.floatToRawIntBits(v));
    }

    public static void writeFloatVolatileL(Object resource, long offset, float v) {
        writeFloatVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, Float.floatToRawIntBits(v));
    }

    public static <R> void writeFloatVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                               long offset, float v) {
        writeIntVolatileB(memoryAccessStrategy, resource, offset, Float.floatToRawIntBits(v));
    }

    public static <R> void writeFloatVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                               long offset, float v) {
        writeIntVolatileL(memoryAccessStrategy, resource, offset, Float.floatToRawIntBits(v));
    }

    //////////////////////////////////////////////////////////////////

    public static long readLongVolatile(long address, boolean bigEndian) {
        return readLongVolatile(null, address, bigEndian);
    }

    public static long readLongVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readLongVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static long readLongVolatile(Object resource, long offset, boolean bigEndian) {
        return readLongVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> long readLongVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                            long offset, boolean bigEndian) {
        if (bigEndian) {
            return readLongVolatileB(memoryAccessStrategy, resource, offset);
        } else {
            return readLongVolatileL(memoryAccessStrategy, resource, offset);
        }
    }

    public static long readLongVolatileB(Object resource, long offset) {
        return readLongVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static long readLongVolatileL(Object resource, long offset) {
        return readLongVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> long readLongVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        long byte7 = (long) memoryAccessStrategy.getByte(resource, offset) << 56;
        long byte6 = (long) (memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF) << 48;
        long byte5 = (long) (memoryAccessStrategy.getByte(resource, offset + 2) & 0xFF) << 40;
        long byte4 = (long) (memoryAccessStrategy.getByte(resource, offset + 3) & 0xFF) << 32;
        long byte3 = (long) (memoryAccessStrategy.getByte(resource, offset + 4) & 0xFF) << 24;
        long byte2 = (long) (memoryAccessStrategy.getByte(resource, offset + 5) & 0xFF) << 16;
        long byte1 = (long) (memoryAccessStrategy.getByte(resource, offset + 6) & 0xFF) << 8;
        long byte0 = (long) (memoryAccessStrategy.getByte(resource, offset + 7) & 0xFF);
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static <R> long readLongVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset) {
        long byte7 = (long) (memoryAccessStrategy.getByte(resource, offset) & 0xFF);
        long byte6 = (long) (memoryAccessStrategy.getByte(resource, offset + 1) & 0xFF) << 8;
        long byte5 = (long) (memoryAccessStrategy.getByte(resource, offset + 2) & 0xFF) << 16;
        long byte4 = (long) (memoryAccessStrategy.getByte(resource, offset + 3) & 0xFF) << 24;
        long byte3 = (long) (memoryAccessStrategy.getByte(resource, offset + 4) & 0xFF) << 32;
        long byte2 = (long) (memoryAccessStrategy.getByte(resource, offset + 5) & 0xFF) << 40;
        long byte1 = (long) (memoryAccessStrategy.getByte(resource, offset + 6) & 0xFF) << 48;
        long byte0 = (long) (memoryAccessStrategy.getByte(resource, offset + 7) & 0xFF) << 56;
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void writeLongVolatile(long address, long v, boolean bigEndian) {
        writeLongVolatile(null, address, v, bigEndian);
    }

    public static void writeLongVolatile(byte[] buffer, int pos, long v, boolean bigEndian) {
        writeLongVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeLongVolatile(Object resource, long offset, long v, boolean bigEndian) {
        writeLongVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeLongVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                             long offset, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongVolatileB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeLongVolatileL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeLongVolatileB(Object resource, long offset, long v) {
        writeLongVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeLongVolatileL(Object resource, long offset, long v) {
        writeLongVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeLongVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                              long offset, long v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) (v >>> 56));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) (v >>> 48));
        memoryAccessStrategy.putByte(resource, offset + 2, (byte) (v >>> 40));
        memoryAccessStrategy.putByte(resource, offset + 3, (byte) (v >>> 32));
        memoryAccessStrategy.putByte(resource, offset + 4, (byte) (v >>> 24));
        memoryAccessStrategy.putByte(resource, offset + 5, (byte) (v >>> 16));
        memoryAccessStrategy.putByte(resource, offset + 6, (byte) (v >>> 8));
        memoryAccessStrategy.putByte(resource, offset + 7, (byte) (v));
    }

    public static <R> void writeLongVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource, long offset, long v) {
        memoryAccessStrategy.putByte(resource, offset, (byte) (v));
        memoryAccessStrategy.putByte(resource, offset + 1, (byte) (v >>> 8));
        memoryAccessStrategy.putByte(resource, offset + 2, (byte) (v >>> 16));
        memoryAccessStrategy.putByte(resource, offset + 3, (byte) (v >>> 24));
        memoryAccessStrategy.putByte(resource, offset + 4, (byte) (v >>> 32));
        memoryAccessStrategy.putByte(resource, offset + 5, (byte) (v >>> 40));
        memoryAccessStrategy.putByte(resource, offset + 6, (byte) (v >>> 48));
        memoryAccessStrategy.putByte(resource, offset + 7, (byte) (v >>> 56));
    }

    //////////////////////////////////////////////////////////////////

    public static double readDoubleVolatile(long address, boolean bigEndian) {
        return readDoubleVolatile(null, address, bigEndian);
    }

    public static double readDoubleVolatile(byte[] buffer, int pos, boolean bigEndian) {
        return readDoubleVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, bigEndian);
    }

    public static double readDoubleVolatile(Object resource, long offset, boolean bigEndian) {
        return readDoubleVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, bigEndian);
    }

    public static <R> double readDoubleVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                                long offset, boolean bigEndian) {
        if (bigEndian) {
            return readDoubleVolatileB(memoryAccessStrategy, resource, offset);
        } else {
            return readDoubleVolatileL(memoryAccessStrategy, resource, offset);
        }
    }

    public static double readDoubleVolatileB(Object resource, long offset) {
        return readDoubleVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static double readDoubleVolatileL(Object resource, long offset) {
        return readDoubleVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset);
    }

    public static <R> double readDoubleVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                                 long offset) {
        return Double.longBitsToDouble(readLongVolatileB(memoryAccessStrategy, resource, offset));
    }

    public static <R> double readDoubleVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                                 long offset) {
        return Double.longBitsToDouble(readLongVolatileL(memoryAccessStrategy, resource, offset));
    }

    //////////////////////////////////////////////////////////////////

    public static void writeDoubleVolatile(long address, double v, boolean bigEndian) {
        writeDoubleVolatile(null, address, v, bigEndian);
    }

    public static void writeDoubleVolatile(byte[] buffer, int pos, double v, boolean bigEndian) {
        writeDoubleVolatile(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, v, bigEndian);
    }

    public static void writeDoubleVolatile(Object resource, long offset, double v, boolean bigEndian) {
        writeDoubleVolatile(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v, bigEndian);
    }

    public static <R> void writeDoubleVolatile(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                               long offset, double v, boolean bigEndian) {
        if (bigEndian) {
            writeDoubleVolatileB(memoryAccessStrategy, resource, offset, v);
        } else {
            writeDoubleVolatileL(memoryAccessStrategy, resource, offset, v);
        }
    }

    public static void writeDoubleVolatileB(Object resource, long offset, double v) {
        writeDoubleVolatileB(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static void writeDoubleVolatileL(Object resource, long offset, double v) {
        writeDoubleVolatileL(OFF_HEAP_MEMORY_ACCESSOR, resource, offset, v);
    }

    public static <R> void writeDoubleVolatileB(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                                long offset, double v) {
        writeLongVolatileB(memoryAccessStrategy, resource, offset, Double.doubleToRawLongBits(v));
    }

    public static <R> void writeDoubleVolatileL(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                                long offset, double v) {
        writeLongVolatileL(memoryAccessStrategy, resource, offset, Double.doubleToRawLongBits(v));
    }


    //////////////////////////////////////////////////////////////////

    public static int writeUtf8Char(byte[] buffer, int pos, int c) {
        return writeUtf8Char(BYTE_ARRAY_MEMORY_ACCESSOR, buffer, pos, c);
    }

    public static int writeUtf8Char(long bufferPointer, long pos, int c) {
        return writeUtf8Char(OFF_HEAP_MEMORY_ACCESSOR, null, bufferPointer + pos, c);
    }

    public static <R> int writeUtf8Char(ByteMemoryAccessStrategy<R> memoryAccessStrategy, R resource,
                                        long pos, int c) {
        if (c <= 0x007F) {
            memoryAccessStrategy.putByte(resource, pos, (byte) c);
            return 1;
        } else if (c > 0x07FF) {
            memoryAccessStrategy.putByte(resource, pos, (byte) (0xE0 | c >> 12 & 0x0F));
            memoryAccessStrategy.putByte(resource, pos + 1, (byte) (0x80 | c >> 6 & 0x3F));
            memoryAccessStrategy.putByte(resource, pos + 2, (byte) (0x80 | c & 0x3F));
            return 3;
        } else {
            memoryAccessStrategy.putByte(resource, pos, (byte) (0xC0 | c >> 6 & 0x1F));
            memoryAccessStrategy.putByte(resource, pos + 1, (byte) (0x80 | c & 0x3F));
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
