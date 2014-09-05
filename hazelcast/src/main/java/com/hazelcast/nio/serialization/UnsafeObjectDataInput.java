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

final class UnsafeObjectDataInput extends ByteArrayObjectDataInput {

    UnsafeObjectDataInput(Data data, SerializationService service) {
        super(data, service);
    }

    UnsafeObjectDataInput(byte[] buffer, SerializationService service) {
        super(buffer, service);
    }

    public char readChar() throws IOException {
        char c = readChar(pos);
        pos += 2;
        return c;
    }

    public char readChar(int position) throws IOException {
        checkAvailable(position, 2);
        return UnsafeHelper.UNSAFE.getChar(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public double readDouble() throws IOException {
        final double d = readDouble(pos);
        pos += 8;
        return d;
    }

    public double readDouble(int position) throws IOException {
        checkAvailable(position, 8);
        return UnsafeHelper.UNSAFE.getDouble(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public float readFloat() throws IOException {
        final float f = readFloat(pos);
        pos += 4;
        return f;
    }

    public float readFloat(int position) throws IOException {
        checkAvailable(position, 4);
        return UnsafeHelper.UNSAFE.getFloat(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public int readInt() throws IOException {
        int i = readInt(pos);
        pos += 4;
        return i;
    }

    public int readInt(int position) throws IOException {
        checkAvailable(position, 4);
        return UnsafeHelper.UNSAFE.getInt(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public long readLong() throws IOException {
        final long l = readLong(pos);
        pos += 8;
        return l;
    }

    public long readLong(int position) throws IOException {
        checkAvailable(position, 8);
        return UnsafeHelper.UNSAFE.getLong(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public short readShort() throws IOException {
        short s = readShort(pos);
        pos += 2;
        return s;
    }

    public short readShort(int position) throws IOException {
        checkAvailable(position, 2);
        return UnsafeHelper.UNSAFE.getShort(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position);
    }

    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            char[] values = new char[len];
            unsafeMemCopy(values, UnsafeHelper.CHAR_ARRAY_BASE_OFFSET, len, UnsafeHelper.CHAR_ARRAY_INDEX_SCALE);
            return values;
        }
        return new char[0];
    }

    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            int[] values = new int[len];
            unsafeMemCopy(values, UnsafeHelper.INT_ARRAY_BASE_OFFSET, len, UnsafeHelper.INT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new int[0];
    }

    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            long[] values = new long[len];
            unsafeMemCopy(values, UnsafeHelper.LONG_ARRAY_BASE_OFFSET, len, UnsafeHelper.LONG_ARRAY_INDEX_SCALE);
            return values;
        }
        return new long[0];
    }

    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            double[] values = new double[len];
            unsafeMemCopy(values, UnsafeHelper.DOUBLE_ARRAY_BASE_OFFSET, len, UnsafeHelper.DOUBLE_ARRAY_INDEX_SCALE);
            return values;
        }
        return new double[0];
    }

    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            float[] values = new float[len];
            unsafeMemCopy(values, UnsafeHelper.FLOAT_ARRAY_BASE_OFFSET, len, UnsafeHelper.FLOAT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new float[0];
    }

    public short[] readShortArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            short[] values = new short[len];
            unsafeMemCopy(values, UnsafeHelper.SHORT_ARRAY_BASE_OFFSET, len, UnsafeHelper.SHORT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new short[0];
    }

    private void unsafeMemCopy(final Object destArray, final long destArrayTypeOffset,
                               final int destArrayLength, final int indexScale) throws IOException {
        if (destArray == null) {
            throw new IllegalArgumentException("Destination array is NULL!");
        }
        if (destArrayLength < 0) {
            throw new NegativeArraySizeException("Destination array length is negative: " + destArrayLength);
        }
        final int len = destArrayLength * indexScale;
        checkAvailable(pos, len);
        UnsafeHelper.UNSAFE.copyMemory(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, destArray, destArrayTypeOffset, len);
        pos += len;
    }

    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }

    private void checkAvailable(int pos, int k) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Negative pos! -> " + pos);
        }
        if ((size - pos) < k) {
            throw new IOException("Cannot read " + k + " bytes!");
        }
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
