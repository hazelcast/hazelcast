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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.ByteAccessStrategy;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.MemoryAccessor;

import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

/**
 * A utility class with methods that read/write multibyte values from/to byte-addressed memory, with
 * explicitly specified endianness. The low-level strategy of reading and writing individual bytes is
 * supplied by an instance of {@link ByteAccessStrategy}.
 */
@SuppressWarnings({"checkstyle:magicnumber", "MagicNumber", "checkstyle:methodcount", "checkstyle:booleanexpressioncomplexity"})
public final class EndiannessUtil {

    /**
     * Accesses bytes in a Java byte array
     */
    public static final ByteAccessStrategy<byte[]> BYTE_ARRAY_ACCESS = ByteArrayAccessStrategy.INSTANCE;

    /**
     * Accesses bytes in a Java ByteBuffer
     */
    public static final ByteAccessStrategy<ByteBuffer> BYTE_BUFFER_ACCESS = ByteBufferAccessStrategy.INSTANCE;

    /**
     * Accesses bytes in CPU's native address space
     */
    public static final ByteAccessStrategy<Object> NATIVE_ACCESS = GlobalMemoryAccessorRegistry.MEM;

    public static final ByteAccessStrategy<MemoryAccessor> CUSTOM_ACCESS = CustomByteAccessStrategy.INSTANCE;

    private EndiannessUtil() {
    }

    public static <R> char readChar(
            ByteAccessStrategy<R> strategy, R resource, long offset, boolean useBigEndian) {
        return useBigEndian ? readCharB(strategy, resource, offset) : readCharL(strategy, resource, offset);
    }

    public static <R> char readCharB(ByteAccessStrategy<R> strategy, R resource, long offset) {
        int byte1 = strategy.getByte(resource, offset) & 0xFF;
        int byte0 = strategy.getByte(resource, offset + 1) & 0xFF;
        return (char) ((byte1 << 8) | byte0);
    }

    public static <R> char readCharL(ByteAccessStrategy<R> strategy, R resource, long offset) {
        int byte1 = strategy.getByte(resource, offset) & 0xFF;
        int byte0 = strategy.getByte(resource, offset + 1) & 0xFF;
        return (char) ((byte0 << 8) | byte1);
    }

    public static <R> void writeChar(
            ByteAccessStrategy<R> strategy, R resource, long offset, char v, boolean useBigEndian) {
        if (useBigEndian) {
            writeCharB(strategy, resource, offset, v);
        } else {
            writeCharL(strategy, resource, offset, v);
        }
    }

    public static <R> void writeCharB(ByteAccessStrategy<R> strategy, R resource, long offset, char v) {
        strategy.putByte(resource, offset, (byte) ((v >>> 8) & 0xFF));
        strategy.putByte(resource, offset + 1, (byte) ((v) & 0xFF));
    }

    public static <R> void writeCharL(ByteAccessStrategy<R> strategy, R resource, long offset, char v) {
        strategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        strategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static <R> short readShort(ByteAccessStrategy<R> strategy, R resource, long offset, boolean useBigEndian) {
        return useBigEndian ? readShortB(strategy, resource, offset) : readShortL(strategy, resource, offset);
    }

    public static <R> short readShortB(ByteAccessStrategy<R> strategy, R resource, long offset) {
        int byte1 = strategy.getByte(resource, offset) & 0xFF;
        int byte0 = strategy.getByte(resource, offset + 1) & 0xFF;
        return (short) ((byte1 << 8) | byte0);
    }

    public static <R> short readShortL(ByteAccessStrategy<R> strategy, R resource, long offset) {
        int byte1 = strategy.getByte(resource, offset) & 0xFF;
        int byte0 = strategy.getByte(resource, offset + 1) & 0xFF;
        return (short) ((byte0 << 8) | byte1);
    }

    public static <R> void writeShort(
            ByteAccessStrategy<R> strategy, R resource, long offset, short v, boolean useBigEndian) {
        if (useBigEndian) {
            writeShortB(strategy, resource, offset, v);
        } else {
            writeShortL(strategy, resource, offset, v);
        }
    }

    public static <R> void writeShortB(ByteAccessStrategy<R> strategy, R resource, long offset, short v) {
        strategy.putByte(resource, offset, (byte) ((v >>> 8) & 0xFF));
        strategy.putByte(resource, offset + 1, (byte) ((v) & 0xFF));
    }

    public static <R> void writeShortL(ByteAccessStrategy<R> strategy, R resource, long offset, short v) {
        strategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        strategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static <R> int readInt(ByteAccessStrategy<R> strategy, R resource, long offset, boolean useBigEndian) {
        return useBigEndian ? readIntB(strategy, resource, offset) : readIntL(strategy, resource, offset);
    }

    public static <R> int readIntB(ByteAccessStrategy<R> strategy, R resource, long offset) {
        int byte3 = (strategy.getByte(resource, offset) & 0xFF) << 24;
        int byte2 = (strategy.getByte(resource, offset + 1) & 0xFF) << 16;
        int byte1 = (strategy.getByte(resource, offset + 2) & 0xFF) << 8;
        int byte0 = strategy.getByte(resource, offset + 3) & 0xFF;
        return byte3 | byte2 | byte1 | byte0;
    }

    public static <R> int readIntL(ByteAccessStrategy<R> strategy, R resource, long offset) {
        int byte3 = strategy.getByte(resource, offset) & 0xFF;
        int byte2 = (strategy.getByte(resource, offset + 1) & 0xFF) << 8;
        int byte1 = (strategy.getByte(resource, offset + 2) & 0xFF) << 16;
        int byte0 = (strategy.getByte(resource, offset + 3) & 0xFF) << 24;
        return byte3 | byte2 | byte1 | byte0;
    }

    public static <R> void writeInt(
            ByteAccessStrategy<R> strategy, R resource, long offset, int v, boolean useBigEndian) {
        if (useBigEndian) {
            writeIntB(strategy, resource, offset, v);
        } else {
            writeIntL(strategy, resource, offset, v);
        }
    }

    public static <R> void writeIntB(ByteAccessStrategy<R> strategy, R resource, long offset, int v) {
        strategy.putByte(resource, offset, (byte) ((v >>> 24) & 0xFF));
        strategy.putByte(resource, offset + 1, (byte) ((v >>> 16) & 0xFF));
        strategy.putByte(resource, offset + 2, (byte) ((v >>> 8) & 0xFF));
        strategy.putByte(resource, offset + 3, (byte) ((v) & 0xFF));
    }

    public static <R> void writeIntL(ByteAccessStrategy<R> strategy, R resource, long offset, int v) {
        strategy.putByte(resource, offset, (byte) ((v) & 0xFF));
        strategy.putByte(resource, offset + 1, (byte) ((v >>> 8) & 0xFF));
        strategy.putByte(resource, offset + 2, (byte) ((v >>> 16) & 0xFF));
        strategy.putByte(resource, offset + 3, (byte) ((v >>> 24) & 0xFF));
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static <R> float readFloat(ByteAccessStrategy<R> strategy, R resource, long offset, boolean useBigEndian) {
        return useBigEndian ? readFloatB(strategy, resource, offset) : readFloatL(strategy, resource, offset);
    }

    public static <R> float readFloatB(ByteAccessStrategy<R> strategy, R resource, long offset) {
        return Float.intBitsToFloat(readIntB(strategy, resource, offset));
    }

    public static <R> float readFloatL(ByteAccessStrategy<R> strategy, R resource, long offset) {
        return Float.intBitsToFloat(readIntL(strategy, resource, offset));
    }

    public static <R> void writeFloat(
            ByteAccessStrategy<R> strategy, R resource, long offset, float v, boolean useBigEndian) {
        if (useBigEndian) {
            writeFloatB(strategy, resource, offset, v);
        } else {
            writeFloatL(strategy, resource, offset, v);
        }
    }

    public static <R> void writeFloatB(ByteAccessStrategy<R> strategy, R resource, long offset, float v) {
        writeIntB(strategy, resource, offset, Float.floatToRawIntBits(v));
    }

    public static <R> void writeFloatL(ByteAccessStrategy<R> strategy, R resource, long offset, float v) {
        writeIntL(strategy, resource, offset, Float.floatToRawIntBits(v));
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static <R> long readLong(ByteAccessStrategy<R> strategy, R resource, long offset, boolean useBigEndian) {
        return useBigEndian ? readLongB(strategy, resource, offset) : readLongL(strategy, resource, offset);
    }

    public static <R> long readLongB(ByteAccessStrategy<R> strategy, R resource, long offset) {
        long byte7 = (long) strategy.getByte(resource, offset) << 56;
        long byte6 = (long) (strategy.getByte(resource, offset + 1) & 0xFF) << 48;
        long byte5 = (long) (strategy.getByte(resource, offset + 2) & 0xFF) << 40;
        long byte4 = (long) (strategy.getByte(resource, offset + 3) & 0xFF) << 32;
        long byte3 = (long) (strategy.getByte(resource, offset + 4) & 0xFF) << 24;
        long byte2 = (long) (strategy.getByte(resource, offset + 5) & 0xFF) << 16;
        long byte1 = (long) (strategy.getByte(resource, offset + 6) & 0xFF) << 8;
        long byte0 = (long) (strategy.getByte(resource, offset + 7) & 0xFF);
        return byte7 | byte6 | byte5 | byte4 | byte3 | byte2 | byte1 | byte0;
    }

    public static <R> long readLongL(ByteAccessStrategy<R> strategy, R resource, long offset) {
        long byte7 = (long) (strategy.getByte(resource, offset) & 0xFF);
        long byte6 = (long) (strategy.getByte(resource, offset + 1) & 0xFF) << 8;
        long byte5 = (long) (strategy.getByte(resource, offset + 2) & 0xFF) << 16;
        long byte4 = (long) (strategy.getByte(resource, offset + 3) & 0xFF) << 24;
        long byte3 = (long) (strategy.getByte(resource, offset + 4) & 0xFF) << 32;
        long byte2 = (long) (strategy.getByte(resource, offset + 5) & 0xFF) << 40;
        long byte1 = (long) (strategy.getByte(resource, offset + 6) & 0xFF) << 48;
        long byte0 = (long) (strategy.getByte(resource, offset + 7) & 0xFF) << 56;
        return byte7 | byte6 | byte5 | byte4 | byte3 | byte2 | byte1 | byte0;
    }

    public static <R> void writeLong(
            ByteAccessStrategy<R> strategy, R resource, long offset, long v, boolean useBigEndian) {
        if (useBigEndian) {
            writeLongB(strategy, resource, offset, v);
        } else {
            writeLongL(strategy, resource, offset, v);
        }
    }

    public static <R> void writeLongB(ByteAccessStrategy<R> strategy, R resource, long offset, long v) {
        strategy.putByte(resource, offset, (byte) (v >>> 56));
        strategy.putByte(resource, offset + 1, (byte) (v >>> 48));
        strategy.putByte(resource, offset + 2, (byte) (v >>> 40));
        strategy.putByte(resource, offset + 3, (byte) (v >>> 32));
        strategy.putByte(resource, offset + 4, (byte) (v >>> 24));
        strategy.putByte(resource, offset + 5, (byte) (v >>> 16));
        strategy.putByte(resource, offset + 6, (byte) (v >>> 8));
        strategy.putByte(resource, offset + 7, (byte) (v));
    }

    public static <R> void writeLongL(ByteAccessStrategy<R> strategy, R resource, long offset, long v) {
        strategy.putByte(resource, offset, (byte) (v));
        strategy.putByte(resource, offset + 1, (byte) (v >>> 8));
        strategy.putByte(resource, offset + 2, (byte) (v >>> 16));
        strategy.putByte(resource, offset + 3, (byte) (v >>> 24));
        strategy.putByte(resource, offset + 4, (byte) (v >>> 32));
        strategy.putByte(resource, offset + 5, (byte) (v >>> 40));
        strategy.putByte(resource, offset + 6, (byte) (v >>> 48));
        strategy.putByte(resource, offset + 7, (byte) (v >>> 56));
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    public static <R> double readDouble(
            ByteAccessStrategy<R> strategy, R resource, long offset, boolean useBigEndian) {
        return useBigEndian ? readDoubleB(strategy, resource, offset) : readDoubleL(strategy, resource, offset);
    }

    public static <R> double readDoubleB(ByteAccessStrategy<R> strategy, R resource, long offset) {
        return Double.longBitsToDouble(readLongB(strategy, resource, offset));
    }

    public static <R> double readDoubleL(ByteAccessStrategy<R> strategy, R resource, long offset) {
        return Double.longBitsToDouble(readLongL(strategy, resource, offset));
    }

    public static <R> void writeDouble(
            ByteAccessStrategy<R> strategy, R resource, long offset, double v, boolean useBigEndian) {
        if (useBigEndian) {
            writeDoubleB(strategy, resource, offset, v);
        } else {
            writeDoubleL(strategy, resource, offset, v);
        }
    }

    public static <R> void writeDoubleB(
            ByteAccessStrategy<R> memoryAccessStrategy, R resource, long offset, double v) {
        writeLongB(memoryAccessStrategy, resource, offset, Double.doubleToRawLongBits(v));
    }

    public static <R> void writeDoubleL(
            ByteAccessStrategy<R> memoryAccessStrategy, R resource, long offset, double v) {
        writeLongL(memoryAccessStrategy, resource, offset, Double.doubleToRawLongBits(v));
    }

    //////////////////////////////////////////////////////////////////

    public static <R> int writeUtf8Char(ByteAccessStrategy<R> strategy, R resource, long pos, int c) {
        if (c <= 0x007F) {
            strategy.putByte(resource, pos, (byte) c);
            return 1;
        } else if (c > 0x07FF) {
            strategy.putByte(resource, pos, (byte) (0xE0 | c >> 12 & 0x0F));
            strategy.putByte(resource, pos + 1, (byte) (0x80 | c >> 6 & 0x3F));
            strategy.putByte(resource, pos + 2, (byte) (0x80 | c & 0x3F));
            return 3;
        } else {
            strategy.putByte(resource, pos, (byte) (0xC0 | c >> 6 & 0x1F));
            strategy.putByte(resource, pos + 1, (byte) (0x80 | c & 0x3F));
            return 2;
        }
    }

    public static char readUtf8CharCompatibility(DataInput in, byte firstByte) throws IOException {
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
}
