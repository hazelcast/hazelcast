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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.UnsafeHelper;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

class UnsafeObjectDataInput extends ByteArrayObjectDataInput {

    UnsafeObjectDataInput(byte[] buffer, SerializationService service) {
        super(buffer, service, ByteOrder.nativeOrder());
    }

    UnsafeObjectDataInput(byte[] buffer, int offset, SerializationService service) {
        super(buffer, offset, service, ByteOrder.nativeOrder());
    }

    @Override
    public int read() throws IOException {
        return (pos < size) ? UnsafeHelper.UNSAFE.getByte(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos++) & 0xFF : -1;
    }

    @Override
    public int read(int position) throws IOException {
        return (position < size) ? UnsafeHelper.UNSAFE
                .getByte(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position) : NULL_ARRAY_LENGTH;
    }

    @Override
    public char readChar(int position) throws IOException {
        checkAvailable(position, CHAR_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getChar(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public double readDouble() throws IOException {
        final double d = readDouble(pos);
        pos += DOUBLE_SIZE_IN_BYTES;
        return d;
    }

    @Override
    public double readDouble(int position) throws IOException {
        checkAvailable(position, DOUBLE_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getDouble(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public float readFloat() throws IOException {
        final float f = readFloat(pos);
        pos += FLOAT_SIZE_IN_BYTES;
        return f;
    }

    @Override
    public float readFloat(int position) throws IOException {
        checkAvailable(position, FLOAT_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getFloat(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public int readInt(int position) throws IOException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getInt(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public int readInt(int position, ByteOrder byteOrder) throws IOException {
        int v = readInt(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Integer.reverseBytes(v);
        }
        return v;
    }

    @Override
    public long readLong(int position) throws IOException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getLong(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public long readLong(int position, ByteOrder byteOrder) throws IOException {
        long v = readLong(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Long.reverseBytes(v);
        }
        return v;
    }

    @Override
    public short readShort(int position) throws IOException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getShort(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public short readShort(int position, ByteOrder byteOrder) throws IOException {
        short v = readShort(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Short.reverseBytes(v);
        }
        return v;
    }

    @Override
    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            char[] values = new char[len];
            memCopy(values, UnsafeHelper.CHAR_ARRAY_BASE_OFFSET, len, UnsafeHelper.CHAR_ARRAY_INDEX_SCALE);
            return values;
        }
        return new char[0];
    }

    @Override
    public boolean[] readBooleanArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            boolean[] values = new boolean[len];
            memCopy(values, UnsafeHelper.BOOLEAN_ARRAY_BASE_OFFSET, len, UnsafeHelper.BOOLEAN_ARRAY_INDEX_SCALE);
            return values;
        }
        return new boolean[0];
    }

    @Override
    public byte[] readByteArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            byte[] values = new byte[len];
            memCopy(values, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET, len, UnsafeHelper.BYTE_ARRAY_INDEX_SCALE);
            return values;
        }
        return new byte[0];
    }

    @Override
    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            int[] values = new int[len];
            memCopy(values, UnsafeHelper.INT_ARRAY_BASE_OFFSET, len, UnsafeHelper.INT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new int[0];
    }

    @Override
    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            long[] values = new long[len];
            memCopy(values, UnsafeHelper.LONG_ARRAY_BASE_OFFSET, len, UnsafeHelper.LONG_ARRAY_INDEX_SCALE);
            return values;
        }
        return new long[0];
    }

    @Override
    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            double[] values = new double[len];
            memCopy(values, UnsafeHelper.DOUBLE_ARRAY_BASE_OFFSET, len, UnsafeHelper.DOUBLE_ARRAY_INDEX_SCALE);
            return values;
        }
        return new double[0];
    }

    @Override
    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            float[] values = new float[len];
            memCopy(values, UnsafeHelper.FLOAT_ARRAY_BASE_OFFSET, len, UnsafeHelper.FLOAT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new float[0];
    }

    @Override
    public short[] readShortArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            short[] values = new short[len];
            memCopy(values, UnsafeHelper.SHORT_ARRAY_BASE_OFFSET, len, UnsafeHelper.SHORT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new short[0];
    }

    private void memCopy(final Object dest, final long destOffset, final int length, final int indexScale) throws IOException {
        if (length < 0) {
            throw new NegativeArraySizeException("Destination length is negative: " + length);
        }

        int remaining = length * indexScale;
        checkAvailable(pos, remaining);
        long offset = destOffset;

        while (remaining > 0) {
            int chunk = (remaining > UnsafeHelper.MEM_COPY_THRESHOLD) ? UnsafeHelper.MEM_COPY_THRESHOLD : remaining;
            UnsafeHelper.UNSAFE.copyMemory(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, dest, offset, chunk);
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
