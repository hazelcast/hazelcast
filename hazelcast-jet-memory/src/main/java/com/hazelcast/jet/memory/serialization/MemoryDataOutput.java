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

package com.hazelcast.jet.memory.serialization;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.memory.impl.EndiannessUtil.CUSTOM_ACCESS;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
 * {@code JetDataOutput} backed by a {@code MemoryManager}-provided memory block. Allocates
 * a new memory block for itself, then reallocates it to expand as needed. Never deallocates memory.
 */
@SuppressWarnings("checkstyle:methodcount")
public class MemoryDataOutput implements ObjectDataOutput {

    public static final int UTF8_LENGTH_SCALE = 3;

    protected long pos;
    protected long bufSize;
    protected long bufBase = NULL_ADDRESS;
    private MemoryAccessor accessor;
    private MemoryAllocator allocator;

    private final SerializationOptimizer optimizer;
    private final boolean isBigEndian;
    private final InternalSerializationService service;

    public MemoryDataOutput(MemoryManager memoryManager, SerializationOptimizer optimizer, boolean isBigEndian) {
        this.service = new DefaultSerializationServiceBuilder().build();
        this.optimizer = optimizer;
        this.isBigEndian = isBigEndian;
        setMemoryManager(memoryManager);
    }

    public void setMemoryManager(MemoryManager memoryManager) {
        if (memoryManager != null) {
            this.allocator = memoryManager.getAllocator();
            this.accessor = memoryManager.getAccessor();
        } else {
            this.allocator = null;
            this.accessor = null;
        }
    }

    public void writeOptimized(Object object) throws IOException {
        optimizer.write(object, this);
    }

    // DataOutput implementation

    @Override
    public void writeObject(Object object) throws IOException {
        service.writeObject(this, object);
    }

    @Override
    public void write(int b) {
        ensureAvailable(1);
        accessor.putByte(bufBase + (pos++), (byte) (b));
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        ensureAvailable(bytes.length);
        accessor.copyFromByteArray(bytes, 0, bufBase + pos, bytes.length);
        pos += bytes.length;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if ((off < 0) || (len < 0) || ((off + len) > b.length)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        ensureAvailable(len);
        accessor.copyFromByteArray(b, off, bufBase + pos, len);
        pos += len;
    }

    @Override
    public final void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public final void writeByte(final int v) throws IOException {
        write(v);
    }

    @Override
    public final void writeBytes(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len);
        for (int i = 0; i < len; i++) {
            accessor.putByte(bufBase + (pos++), (byte) s.charAt(i));
        }
    }

    @Override
    public void writeChar(final int v) throws IOException {
        ensureAvailable(CHAR_SIZE_IN_BYTES);
        EndiannessUtil.writeChar(CUSTOM_ACCESS, accessor, bufBase + pos, (char) v, isBigEndian);
        pos += CHAR_SIZE_IN_BYTES;
    }

    @Override
    public void writeChars(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len * CHAR_SIZE_IN_BYTES);
        for (int i = 0; i < len; i++) {
            final int v = s.charAt(i);
            EndiannessUtil.writeChar(CUSTOM_ACCESS, accessor, bufBase + pos, (char) v, isBigEndian);
            pos += CHAR_SIZE_IN_BYTES;
        }
    }

    @Override
    public void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeInt(final int v) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        EndiannessUtil.writeInt(CUSTOM_ACCESS, accessor, bufBase + pos, v, isBigEndian);
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(final long v) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        EndiannessUtil.writeLong(CUSTOM_ACCESS, accessor, bufBase + pos, v, isBigEndian);
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(final int v) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        EndiannessUtil.writeShort(CUSTOM_ACCESS, accessor, bufBase + pos, (short) v, isBigEndian);
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeUTF(final String str) throws IOException {
        int len = (str != null) ? str.length() : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            ensureAvailable(UTF8_LENGTH_SCALE * len);
            for (int i = 0; i < len; i++) {
                pos += EndiannessUtil.writeUtf8Char(CUSTOM_ACCESS, accessor, bufBase + pos, str.charAt(i));
            }
        }
    }

    @Override
    public void writeByteArray(byte[] bytes) throws IOException {
        int len = (bytes != null) ? bytes.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            write(bytes);
        }
    }

    @Override
    public void writeBooleanArray(boolean[] booleans) throws IOException {
        int len = (booleans != null) ? booleans.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (boolean b : booleans) {
                writeBoolean(b);
            }
        }
    }

    @Override
    public void writeCharArray(char[] chars) throws IOException {
        int len = chars != null ? chars.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (char c : chars) {
                writeChar(c);
            }
        }
    }

    @Override
    public void writeIntArray(int[] ints) throws IOException {
        int len = ints != null ? ints.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (int i : ints) {
                writeInt(i);
            }
        }
    }

    @Override
    public void writeLongArray(long[] longs) throws IOException {
        int len = longs != null ? longs.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (long l : longs) {
                writeLong(l);
            }
        }
    }

    @Override
    public void writeDoubleArray(double[] doubles) throws IOException {
        int len = doubles != null ? doubles.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (double d : doubles) {
                writeDouble(d);
            }
        }
    }

    @Override
    public void writeFloatArray(float[] floats) throws IOException {
        int len = floats != null ? floats.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (float f : floats) {
                writeFloat(f);
            }
        }
    }

    @Override
    public void writeShortArray(short[] shorts) throws IOException {
        int len = shorts != null ? shorts.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (short s : shorts) {
                writeShort(s);
            }
        }
    }

    @Override
    public void writeUTFArray(String[] strings) throws IOException {
        int len = strings != null ? strings.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (String s : strings) {
                writeUTF(s);
            }
        }
    }

    @Override
    public void writeData(Data data) throws IOException {
        byte[] payload = data != null ? data.toByteArray() : null;
        writeByteArray(payload);
    }

    @Override
    public byte[] toByteArray() {
        throw new IllegalStateException("Not available for unified serialization");
    }

    @Override
    public byte[] toByteArray(int padding) {
        return new byte[0];
    }

    @Override
    public ByteOrder getByteOrder() {
        return isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString() {
        return "JetByteArrayObjectDataOutput{"
                + "size=" + (bufBase != NULL_ADDRESS ? bufSize : 0) + ", pos=" + pos + '}';
    }



    // Custom methods

    public final long position() {
        return pos;
    }

    public long baseAddress() {
        return bufBase;
    }

    public long usedSize() {
        return pos;
    }

    public long allocatedSize() {
        return bufSize;
    }

    public void skip(long delta) {
        ensureAvailable(delta);
        pos += delta;
    }

    public void clear() {
        pos = 0;
        bufSize = 0;
        bufBase = NULL_ADDRESS;
    }

    private void ensureAvailable(long len) {
        if (bufBase == NULL_ADDRESS) {
            bufBase = allocator.allocate(len);
            bufSize = len;
            return;
        }
        if (bufSize - pos >= len) {
            return;
        }
        bufBase = allocator.reallocate(bufBase, bufSize, bufSize + len);
        bufSize += len;
    }
}
