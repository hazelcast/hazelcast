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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.UnsafeHelper;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

class UnsafeObjectDataInput extends ByteArrayObjectDataInput {

    UnsafeObjectDataInput(Data data, SerializationService service) {
        super(data, service, ByteOrder.nativeOrder());
    }

    UnsafeObjectDataInput(byte[] buffer, SerializationService service) {
        super(buffer, service, ByteOrder.nativeOrder());
    }

    public int read() throws IOException {
        return (pos < size) ? UnsafeHelper.UNSAFE.getByte(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos++) : -1;
    }

    public int read(int position) throws IOException {
        return (position < size) ? UnsafeHelper.UNSAFE.getByte(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position) : -1;
    }

    public char readChar(int position) throws IOException {
        checkAvailable(position, CHAR_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getChar(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public double readDouble() throws IOException {
        final double d = readDouble(pos);
        pos += DOUBLE_SIZE_IN_BYTES;
        return d;
    }

    public double readDouble(int position) throws IOException {
        checkAvailable(position, DOUBLE_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getDouble(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public float readFloat() throws IOException {
        final float f = readFloat(pos);
        pos += FLOAT_SIZE_IN_BYTES;
        return f;
    }

    public float readFloat(int position) throws IOException {
        checkAvailable(position, FLOAT_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getFloat(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public int readInt(int position) throws IOException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getInt(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public long readLong(int position) throws IOException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getLong(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public short readShort(int position) throws IOException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        return UnsafeHelper.UNSAFE.getShort(data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            char[] values = new char[len];
            memCopy(values, UnsafeHelper.CHAR_ARRAY_BASE_OFFSET, len, UnsafeHelper.CHAR_ARRAY_INDEX_SCALE);
            return values;
        }
        return new char[0];
    }

    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            int[] values = new int[len];
            memCopy(values, UnsafeHelper.INT_ARRAY_BASE_OFFSET, len, UnsafeHelper.INT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new int[0];
    }

    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            long[] values = new long[len];
            memCopy(values, UnsafeHelper.LONG_ARRAY_BASE_OFFSET, len, UnsafeHelper.LONG_ARRAY_INDEX_SCALE);
            return values;
        }
        return new long[0];
    }

    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            double[] values = new double[len];
            memCopy(values, UnsafeHelper.DOUBLE_ARRAY_BASE_OFFSET, len, UnsafeHelper.DOUBLE_ARRAY_INDEX_SCALE);
            return values;
        }
        return new double[0];
    }

    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            float[] values = new float[len];
            memCopy(values, UnsafeHelper.FLOAT_ARRAY_BASE_OFFSET, len, UnsafeHelper.FLOAT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new float[0];
    }

    public short[] readShortArray() throws IOException {
        int len = readInt();
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

    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("UnsafeObjectDataInput");
        sb.append("{size=").append(size);
        sb.append(", pos=").append(pos);
        sb.append(", mark=").append(mark);
        sb.append(", byteOrder=").append(getByteOrder());
        sb.append('}');
        return sb.toString();
    }
}
