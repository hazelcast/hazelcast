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

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.serialization.InternalSerializationService;

import java.io.IOException;
import java.nio.ByteOrder;

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

class UnsafeObjectDataOutput extends ByteArrayObjectDataOutput {

    UnsafeObjectDataOutput(int size, InternalSerializationService service) {
        super(size, service, ByteOrder.nativeOrder());
    }

    UnsafeObjectDataOutput(int initialSize, int firstGrowthSize, InternalSerializationService service) {
        super(initialSize, firstGrowthSize, service, ByteOrder.nativeOrder());
    }

    @Override
    public void writeChar(final int v) throws IOException {
        ensureAvailable(CHAR_SIZE_IN_BYTES);
        MEM.putChar(buffer, ARRAY_BYTE_BASE_OFFSET + pos, (char) v);
        pos += CHAR_SIZE_IN_BYTES;
    }

    @Override
    public void writeChar(int position, final int v) throws IOException {
        checkAvailable(position, CHAR_SIZE_IN_BYTES);
        MEM.putChar(buffer, ARRAY_BYTE_BASE_OFFSET + position, (char) v);
    }

    @Override
    public void writeDouble(final double v) throws IOException {
        ensureAvailable(DOUBLE_SIZE_IN_BYTES);
        MEM.putDouble(buffer, ARRAY_BYTE_BASE_OFFSET + pos, v);
        pos += DOUBLE_SIZE_IN_BYTES;
    }

    @Override
    public void writeDouble(int position, final double v) throws IOException {
        checkAvailable(position, DOUBLE_SIZE_IN_BYTES);
        MEM.putDouble(buffer, ARRAY_BYTE_BASE_OFFSET + position, v);
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        ensureAvailable(FLOAT_SIZE_IN_BYTES);
        MEM.putFloat(buffer, ARRAY_BYTE_BASE_OFFSET + pos, v);
        pos += FLOAT_SIZE_IN_BYTES;
    }

    @Override
    public void writeFloat(int position, final float v) throws IOException {
        checkAvailable(position, FLOAT_SIZE_IN_BYTES);
        MEM.putFloat(buffer, ARRAY_BYTE_BASE_OFFSET + position, v);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        MEM.putInt(buffer, ARRAY_BYTE_BASE_OFFSET + pos, v);
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(int position, int v) throws IOException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        MEM.putInt(buffer, ARRAY_BYTE_BASE_OFFSET + position, v);
    }

    @Override
    public void writeInt(int v, ByteOrder byteOrder) throws IOException {
        if (byteOrder != ByteOrder.nativeOrder()) {
            writeInt(Integer.reverseBytes(v));
        } else {
            writeInt(v);
        }
    }

    @Override
    public void writeInt(int position, int v, ByteOrder byteOrder) throws IOException {
        if (byteOrder != ByteOrder.nativeOrder()) {
            writeInt(position, Integer.reverseBytes(v));
        } else {
            writeInt(position, v);
        }
    }

    @Override
    public void writeLong(final long v) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        MEM.putLong(buffer, ARRAY_BYTE_BASE_OFFSET + pos, v);
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(int position, final long v) throws IOException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        MEM.putLong(buffer, ARRAY_BYTE_BASE_OFFSET + position, v);
    }

    @Override
    public void writeLong(long v, ByteOrder byteOrder) throws IOException {
        if (byteOrder != ByteOrder.nativeOrder()) {
            writeLong(Long.reverseBytes(v));
        } else {
            writeLong(v);
        }
    }

    @Override
    public void writeLong(int position, long v, ByteOrder byteOrder) throws IOException {
        if (byteOrder != ByteOrder.nativeOrder()) {
            writeLong(position, Long.reverseBytes(v));
        } else {
            writeLong(position, v);
        }
    }

    @Override
    public void writeShort(final int v) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        MEM.putShort(buffer, ARRAY_BYTE_BASE_OFFSET + pos, (short) v);
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(int position, final int v) throws IOException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        MEM.putShort(buffer, ARRAY_BYTE_BASE_OFFSET + position, (short) v);
    }

    @Override
    public void writeShort(int v, ByteOrder byteOrder) throws IOException {
        short s = (short) v;
        if (byteOrder != ByteOrder.nativeOrder()) {
            writeShort(Short.reverseBytes(s));
        } else {
            writeShort(v);
        }
    }

    @Override
    public void writeShort(int position, int v, ByteOrder byteOrder) throws IOException {
        short s = (short) v;
        if (byteOrder != ByteOrder.nativeOrder()) {
            writeShort(position, Short.reverseBytes(s));
        } else {
            writeShort(position, v);
        }
    }

    @Override
    public void writeBooleanArray(boolean[] booleans) throws IOException {
        int len = (booleans != null) ? booleans.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            memCopy(booleans, ARRAY_BOOLEAN_BASE_OFFSET,
                    len, ARRAY_BOOLEAN_INDEX_SCALE);
        }
    }

    @Override
    public void writeByteArray(byte[] bytes) throws IOException {
        int len = (bytes != null) ? bytes.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            memCopy(bytes, ARRAY_BYTE_BASE_OFFSET, len, ARRAY_BYTE_INDEX_SCALE);
        }
    }

    @Override
    public void writeCharArray(char[] values) throws IOException {
        int len = values != null ? values.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            memCopy(values, ARRAY_CHAR_BASE_OFFSET, len, ARRAY_CHAR_INDEX_SCALE);
        }
    }

    @Override
    public void writeShortArray(short[] values) throws IOException {
        int len = values != null ? values.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            memCopy(values, ARRAY_SHORT_BASE_OFFSET, len, ARRAY_SHORT_INDEX_SCALE);
        }
    }

    @Override
    public void writeIntArray(int[] values) throws IOException {
        int len = values != null ? values.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            memCopy(values, ARRAY_INT_BASE_OFFSET, len, ARRAY_INT_INDEX_SCALE);
        }
    }

    @Override
    public void writeFloatArray(float[] values) throws IOException {
        int len = values != null ? values.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            memCopy(values, ARRAY_FLOAT_BASE_OFFSET, len, ARRAY_FLOAT_INDEX_SCALE);
        }
    }

    @Override
    public void writeLongArray(long[] values) throws IOException {
        int len = values != null ? values.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            memCopy(values, ARRAY_LONG_BASE_OFFSET, len, ARRAY_LONG_INDEX_SCALE);
        }
    }

    @Override
    public void writeDoubleArray(double[] values) throws IOException {
        int len = values != null ? values.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            memCopy(values, ARRAY_DOUBLE_BASE_OFFSET, len, ARRAY_DOUBLE_INDEX_SCALE);
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
            int chunk = Math.min(remaining, GlobalMemoryAccessor.MEM_COPY_THRESHOLD);
            MEM.copyMemory(src, offset, buffer, ARRAY_BYTE_BASE_OFFSET + pos, chunk);
            remaining -= chunk;
            offset += chunk;
            pos += chunk;
        }
    }

    @Override
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
        return "UnsafeObjectDataOutput{"
                + "size=" + (buffer != null ? buffer.length : 0)
                + ", pos=" + pos
                + ", byteOrder=" + getByteOrder()
                + '}';
    }
}
