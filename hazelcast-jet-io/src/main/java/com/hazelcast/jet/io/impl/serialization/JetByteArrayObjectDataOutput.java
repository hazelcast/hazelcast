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

package com.hazelcast.jet.io.impl.serialization;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.io.serialization.JetSerializationService;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
 * Provides methods which let to serialize data directly to  some byte-represented part of memory
 * <p>
 * It works like a factory for the memory-blocks creating and returning pointer and sizes
 * It doesn't release memory, memory-releasing is responsibility of the external environment
 */
@SuppressWarnings({
        "checkstyle:methodcount",
        "checkstyle:magicnumber"
})
class JetByteArrayObjectDataOutput implements JetDataOutput {

    protected long pos;
    protected long bufferSize;
    protected long bufferPointer = MemoryAllocator.NULL_ADDRESS;

    private final boolean isBigEndian;
    private MemoryManager memoryManager;
    private final JetSerializationService service;

    JetByteArrayObjectDataOutput(MemoryManager memoryManager,
                                 JetSerializationService service,
                                 ByteOrder byteOrder) {
        this.service = service;
        this.memoryManager = memoryManager;
        this.isBigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
    }

    @Override
    public void write(int b) {
        ensureAvailable(1);
        memoryManager.getAccessor().putByte(bufferPointer + (pos++), (byte) (b));
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        ensureAvailable(bytes.length);
        memoryManager.getAccessor().copyFromByteArray(bytes, 0, bufferPointer + pos, bytes.length);
        pos += bytes.length;
    }

    @Override
    public void write(long position, int b) {
        MEM.putByte(bufferPointer + position, (byte) (b));
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if ((off < 0) || (len < 0) || ((off + len) > b.length)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        ensureAvailable(len);
        memoryManager.getAccessor().copyFromByteArray(b, off, bufferPointer + pos, len);
        pos += len;
    }

    @Override
    public final void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public final void writeBoolean(long position, final boolean v) throws IOException {
        write(position, v ? 1 : 0);
    }

    @Override
    public final void writeByte(final int v) throws IOException {
        write(v);
    }

    @Override
    public final void writeZeroBytes(int count) {
        for (int k = 0; k < count; k++) {
            write(0);
        }
    }

    @Override
    public final void writeByte(long position, final int v) throws IOException {
        write(position, v);
    }

    @Override
    public final void writeBytes(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len);
        for (int i = 0; i < len; i++) {
            MEM.putByte(bufferPointer + (pos++), (byte) s.charAt(i));
        }
    }

    @Override
    public void writeChar(final int v) throws IOException {
        ensureAvailable(CHAR_SIZE_IN_BYTES);
        EndiannessUtil.writeChar(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, (char) v, isBigEndian
        );
        pos += CHAR_SIZE_IN_BYTES;
    }

    @Override
    public void writeChar(long position, final int v) throws IOException {
        EndiannessUtil.writeChar(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + position, (char) v, isBigEndian
        );
    }

    @Override
    public void writeChars(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len * CHAR_SIZE_IN_BYTES);
        for (int i = 0; i < len; i++) {
            final int v = s.charAt(i);
            writeChar(pos, v);
            pos += CHAR_SIZE_IN_BYTES;
        }
    }

    @Override
    public void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeDouble(long position, final double v) throws IOException {
        writeLong(position, Double.doubleToLongBits(v));
    }

    @Override
    public void writeDouble(double v, ByteOrder byteOrder) throws IOException {
        writeLong(Double.doubleToLongBits(v), byteOrder);
    }

    @Override
    public void writeDouble(long position, double v, ByteOrder byteOrder) throws IOException {
        writeLong(position, Double.doubleToLongBits(v), byteOrder);
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeFloat(long position, final float v) throws IOException {
        writeInt(position, Float.floatToIntBits(v));
    }

    @Override
    public void writeFloat(float v, ByteOrder byteOrder) throws IOException {
        writeInt(Float.floatToIntBits(v), byteOrder);
    }

    @Override
    public void writeFloat(long position, float v, ByteOrder byteOrder) throws IOException {
        writeInt(position, Float.floatToIntBits(v), byteOrder);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        EndiannessUtil.writeInt(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, v, isBigEndian
        );
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(long position, int v) throws IOException {
        EndiannessUtil.writeInt(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + position, v, isBigEndian
        );
    }

    @Override
    public void writeInt(int v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        EndiannessUtil.writeInt(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, v, byteOrder == ByteOrder.BIG_ENDIAN
        );
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(long position, int v, ByteOrder byteOrder) throws IOException {
        EndiannessUtil.writeInt(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + position, v, byteOrder == ByteOrder.BIG_ENDIAN
        );
    }

    @Override
    public void writeLong(final long v) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        EndiannessUtil.writeLong(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, v, isBigEndian
        );
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(long position, final long v) throws IOException {
        EndiannessUtil.writeLong(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, v, isBigEndian
        );
    }

    @Override
    public void writeLong(long v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        EndiannessUtil.writeLong(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, v, isBigEndian
        );
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(long position, long v, ByteOrder byteOrder) throws IOException {
        EndiannessUtil.writeLong(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, v, isBigEndian
        );
    }

    @Override
    public void writeShort(final int v) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        EndiannessUtil.writeShort(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, (short) v, isBigEndian
        );
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(long position, final int v) throws IOException {
        EndiannessUtil.writeShort(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, (short) v, isBigEndian
        );
    }

    @Override
    public void writeShort(int v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        EndiannessUtil.writeShort(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, (short) v, isBigEndian
        );
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(long position, int v, ByteOrder byteOrder) throws IOException {
        EndiannessUtil.writeShort(
                EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                bufferPointer + pos, (short) v, isBigEndian
        );
    }

    @Override
    public void writeUTF(final String str) throws IOException {
        int len = (str != null) ? str.length() : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            ensureAvailable(len * 3);
            for (int i = 0; i < len; i++) {
                pos += EndiannessUtil.writeUtf8Char(
                        EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                        bufferPointer + pos, str.charAt(i)
                );
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

    final void ensureAvailable(long len) {
        if (available() < len) {
            if (bufferPointer != MemoryAllocator.NULL_ADDRESS) {
                bufferPointer = memoryManager.getAllocator().reallocate(
                        bufferPointer,
                        bufferSize,
                        bufferSize + len
                );
                bufferSize += len;
            } else {
                bufferSize = len;
                bufferPointer = memoryManager.getAllocator().allocate(len);
            }
        }
    }

    @Override
    public void writeObject(Object object) throws IOException {
        service.writeObject(this, object);
    }

    @Override
    public void writeData(Data data) throws IOException {
        byte[] payload = data != null ? data.toByteArray() : null;
        writeByteArray(payload);
    }

    /**
     * Returns this buffer's position.
     */
    @Override
    public final long position() {
        return pos;
    }

    @Override
    public void position(long newPos) {
        if ((newPos > bufferSize) || (newPos < 0)) {
            throw new IllegalArgumentException();
        }

        pos = newPos;
    }

    @Override
    public void stepOn(long delta) {
        ensureAvailable(delta);
        pos += delta;
    }

    @Override
    public void setMemoryManager(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    public long available() {
        return bufferPointer != MemoryAllocator.NULL_ADDRESS ? bufferSize - pos : 0;
    }

    @Override
    public byte toByteArray()[] {
        throw new IllegalStateException("Not available for unified serialization");
    }

    @Override
    public void clear() {
        pos = 0;
        bufferSize = 0;
        bufferPointer = MemoryAllocator.NULL_ADDRESS;
    }

    @Override
    public ByteOrder getByteOrder() {
        return isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString() {
        return "JetByteArrayObjectDataOutput{"
                + "size=" + (bufferPointer != MemoryAllocator.NULL_ADDRESS ? bufferSize : 0)
                + ", pos=" + pos
                + '}';
    }

    @Override
    public long getPointer() {
        return bufferPointer;
    }

    @Override
    public long getWrittenSize() {
        return pos;
    }

    @Override
    public long getAllocatedSize() {
        return bufferSize;
    }
}
