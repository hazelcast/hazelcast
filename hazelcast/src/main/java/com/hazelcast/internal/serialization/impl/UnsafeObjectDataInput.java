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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;

import java.io.EOFException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.GlobalMemoryAccessor.MEM_COPY_THRESHOLD;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BOOLEAN_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BOOLEAN_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_CHAR_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_CHAR_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_DOUBLE_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_DOUBLE_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_FLOAT_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_FLOAT_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_INT_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_INT_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_LONG_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_LONG_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_SHORT_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_SHORT_INDEX_SCALE;
import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;

class UnsafeObjectDataInput extends ByteArrayObjectDataInput {

    UnsafeObjectDataInput(byte[] buffer,
                          InternalSerializationService service) {
        super(buffer, service, ByteOrder.nativeOrder(), false);
    }

    UnsafeObjectDataInput(byte[] buffer,
                          InternalSerializationService service,
                          boolean isCompatibility) {
        super(buffer, service, ByteOrder.nativeOrder(), isCompatibility);
    }

    UnsafeObjectDataInput(byte[] buffer,
                          int offset,
                          InternalSerializationService service) {
        super(buffer, offset, service, ByteOrder.nativeOrder(), false);
    }

    UnsafeObjectDataInput(byte[] buffer,
                          int offset,
                          InternalSerializationService service,
                          boolean isCompatibility) {
        super(buffer, offset, service, ByteOrder.nativeOrder(), isCompatibility);
    }

    @Override
    public int read() {
        return (pos < size) ? MEM.getByte(data, ARRAY_BYTE_BASE_OFFSET + pos++) & 0xFF : -1;
    }

    @Override
    public int read(int position) {
        return (position < size) ? MEM.getByte(data, ARRAY_BYTE_BASE_OFFSET + position) & 0xFF : NULL_ARRAY_LENGTH;
    }

    @Override
    public char readChar(int position) throws EOFException {
        checkAvailable(position, CHAR_SIZE_IN_BYTES);
        return MEM.getChar(data, ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public double readDouble() throws EOFException {
        final double d = readDouble(pos);
        pos += DOUBLE_SIZE_IN_BYTES;
        return d;
    }

    @Override
    public double readDouble(int position) throws EOFException {
        checkAvailable(position, DOUBLE_SIZE_IN_BYTES);
        return MEM.getDouble(data, ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public float readFloat() throws EOFException {
        final float f = readFloat(pos);
        pos += FLOAT_SIZE_IN_BYTES;
        return f;
    }

    @Override
    public float readFloat(int position) throws EOFException {
        checkAvailable(position, FLOAT_SIZE_IN_BYTES);
        return MEM.getFloat(data, ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public int readInt(int position) throws EOFException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return MEM.getInt(data, ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public int readInt(int position, ByteOrder byteOrder) throws EOFException {
        int v = readInt(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Integer.reverseBytes(v);
        }
        return v;
    }

    @Override
    public long readLong(int position) throws EOFException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return MEM.getLong(data, ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public long readLong(int position, ByteOrder byteOrder) throws EOFException {
        long v = readLong(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Long.reverseBytes(v);
        }
        return v;
    }

    @Override
    public short readShort(int position) throws EOFException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        return MEM.getShort(data, ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public short readShort(int position, ByteOrder byteOrder) throws EOFException {
        short v = readShort(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Short.reverseBytes(v);
        }
        return v;
    }

    @Override
    public char[] readCharArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            char[] values = new char[len];
            memCopy(values, ARRAY_CHAR_BASE_OFFSET, len, ARRAY_CHAR_INDEX_SCALE);
            return values;
        }
        return new char[0];
    }

    @Override
    public boolean[] readBooleanArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            boolean[] values = new boolean[len];
            memCopy(values, ARRAY_BOOLEAN_BASE_OFFSET, len, ARRAY_BOOLEAN_INDEX_SCALE);
            return values;
        }
        return new boolean[0];
    }

    @Override
    public byte[] readByteArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            byte[] values = new byte[len];
            memCopy(values, ARRAY_BYTE_BASE_OFFSET, len, ARRAY_BYTE_INDEX_SCALE);
            return values;
        }
        return new byte[0];
    }

    @Override
    public int[] readIntArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            int[] values = new int[len];
            memCopy(values, ARRAY_INT_BASE_OFFSET, len, ARRAY_INT_INDEX_SCALE);
            return values;
        }
        return new int[0];
    }

    @Override
    public long[] readLongArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            long[] values = new long[len];
            memCopy(values, ARRAY_LONG_BASE_OFFSET, len, ARRAY_LONG_INDEX_SCALE);
            return values;
        }
        return new long[0];
    }

    @Override
    public double[] readDoubleArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            double[] values = new double[len];
            memCopy(values, ARRAY_DOUBLE_BASE_OFFSET, len, ARRAY_DOUBLE_INDEX_SCALE);
            return values;
        }
        return new double[0];
    }

    @Override
    public float[] readFloatArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            float[] values = new float[len];
            memCopy(values, ARRAY_FLOAT_BASE_OFFSET, len, ARRAY_FLOAT_INDEX_SCALE);
            return values;
        }
        return new float[0];
    }

    @Override
    public short[] readShortArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            short[] values = new short[len];
            memCopy(values, ARRAY_SHORT_BASE_OFFSET, len, ARRAY_SHORT_INDEX_SCALE);
            return values;
        }
        return new short[0];
    }

    private void memCopy(final Object dest, final long destOffset, final int length, final int indexScale)
            throws EOFException {
        if (length < 0) {
            throw new NegativeArraySizeException("Destination length is negative: " + length);
        }

        int remaining = length * indexScale;
        checkAvailable(pos, remaining);
        long offset = destOffset;

        while (remaining > 0) {
            int chunk = (remaining > MEM_COPY_THRESHOLD) ? MEM_COPY_THRESHOLD : remaining;
            MEM.copyMemory(data, ARRAY_BYTE_BASE_OFFSET + pos, dest, offset, chunk);
            remaining -= chunk;
            offset += chunk;
            pos += chunk;
        }
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }

    @Override
    public String toString() {
        return "UnsafeObjectDataInput{"
                + "size=" + size
                + ", pos=" + pos
                + ", mark=" + mark
                + ", byteOrder=" + getByteOrder()
                + '}';
    }
}
