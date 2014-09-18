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

class UnsafeObjectDataOutput extends ByteArrayObjectDataOutput {

    UnsafeObjectDataOutput(int size, SerializationService service) {
        super(size, service, ByteOrder.nativeOrder());
    }

    public void writeChar(final int v) throws IOException {
        ensureAvailable(2);
        UnsafeHelper.UNSAFE.putChar(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, (char) v);
        pos += 2;
    }

    public void writeChar(int position, final int v) throws IOException {
        checkAvailable(position, 2);
        UnsafeHelper.UNSAFE.putChar(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position, (char) v);
    }

    public void writeDouble(final double v) throws IOException {
        ensureAvailable(8);
        UnsafeHelper.UNSAFE.putDouble(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, v);
        pos += 8;
    }

    public void writeDouble(int position, final double v) throws IOException {
        checkAvailable(position, 8);
        UnsafeHelper.UNSAFE.putDouble(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position, v);
    }

    public void writeFloat(final float v) throws IOException {
        ensureAvailable(4);
        UnsafeHelper.UNSAFE.putFloat(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, v);
        pos += 4;
    }

    public void writeFloat(int position, final float v) throws IOException {
        checkAvailable(position, 4);
        UnsafeHelper.UNSAFE.putFloat(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position, v);
    }

    public void writeInt(final int v) throws IOException {
        ensureAvailable(4);
        UnsafeHelper.UNSAFE.putInt(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, v);
        pos += 4;
    }

    public void writeInt(int position, int v) throws IOException {
        checkAvailable(position, 4);
        UnsafeHelper.UNSAFE.putInt(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position, v);
    }

    public void writeLong(final long v) throws IOException {
        ensureAvailable(8);
        UnsafeHelper.UNSAFE.putLong(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, v);
        pos += 8;
    }

    public void writeLong(int position, final long v) throws IOException {
        checkAvailable(position, 8);
        UnsafeHelper.UNSAFE.putLong(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position, v);
    }

    public void writeShort(final int v) throws IOException {
        ensureAvailable(2);
        UnsafeHelper.UNSAFE.putShort(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, (short) v);
        pos += 2;
    }

    public void writeShort(int position, final int v) throws IOException {
        checkAvailable(position, 2);
        UnsafeHelper.UNSAFE.putShort(buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + position, (short) v);
    }

    public void writeCharArray(char[] values) throws IOException {
        int len = values != null ? values.length : 0;
        writeInt(len);
        if (len > 0) {
            memCopy(values, UnsafeHelper.CHAR_ARRAY_BASE_OFFSET, len, UnsafeHelper.CHAR_ARRAY_INDEX_SCALE);
        }
    }

    public void writeShortArray(short[] values) throws IOException {
        int len = values != null ? values.length : 0;
        writeInt(len);
        if (len > 0) {
            memCopy(values, UnsafeHelper.SHORT_ARRAY_BASE_OFFSET, len, UnsafeHelper.SHORT_ARRAY_INDEX_SCALE);
        }
    }

    public void writeIntArray(int[] values) throws IOException {
        int len = values != null ? values.length : 0;
        writeInt(len);
        if (len > 0) {
            memCopy(values, UnsafeHelper.INT_ARRAY_BASE_OFFSET, len, UnsafeHelper.INT_ARRAY_INDEX_SCALE);
        }
    }

    public void writeFloatArray(float[] values) throws IOException {
        int len = values != null ? values.length : 0;
        writeInt(len);
        if (len > 0) {
            memCopy(values, UnsafeHelper.FLOAT_ARRAY_BASE_OFFSET, len, UnsafeHelper.FLOAT_ARRAY_INDEX_SCALE);
        }
    }

    public void writeLongArray(long[] values) throws IOException {
        int len = values != null ? values.length : 0;
        writeInt(len);
        if (len > 0) {
            memCopy(values, UnsafeHelper.LONG_ARRAY_BASE_OFFSET, len, UnsafeHelper.LONG_ARRAY_INDEX_SCALE);
        }
    }

    public void writeDoubleArray(double[] values) throws IOException {
        int len = values != null ? values.length : 0;
        writeInt(len);
        if (len > 0) {
            memCopy(values, UnsafeHelper.DOUBLE_ARRAY_BASE_OFFSET, len, UnsafeHelper.DOUBLE_ARRAY_INDEX_SCALE);
        }
    }

    private void memCopy(final Object src, final long srcOffset, final int length, final int indexScale) {
        if (length < 0) {
            throw new NegativeArraySizeException("Source length is negative: " + length);
        }

        int remaining = indexScale * length;
        long offset = srcOffset;
        ensureAvailable(remaining);

        while (remaining > 0) {
            int chunk = (remaining > UnsafeHelper.MEM_COPY_THRESHOLD) ? UnsafeHelper.MEM_COPY_THRESHOLD : remaining;
            UnsafeHelper.UNSAFE.copyMemory(src, offset, buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, chunk);
            remaining -= chunk;
            offset += chunk;
            pos += chunk;
        }
    }

    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }

    private void checkAvailable(int pos, int k) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Negative pos! -> " + pos);
        }
        int size = buffer != null ? buffer.length : 0;
        if ((size - pos) < k) {
            throw new IOException("Cannot write " + k + " bytes!");
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("UnsafeObjectDataOutput");
        sb.append("{size=").append(buffer != null ? buffer.length : 0);
        sb.append(", pos=").append(pos);
        sb.append(", byteOrder=").append(getByteOrder());
        sb.append('}');
        return sb.toString();
    }
}
