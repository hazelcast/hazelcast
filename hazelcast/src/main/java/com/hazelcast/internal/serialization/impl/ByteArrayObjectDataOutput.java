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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.collection.ArrayUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.version.Version.UNKNOWN;

public class ByteArrayObjectDataOutput extends VersionedObjectDataOutput implements BufferObjectDataOutput {

    final int initialSize;

    final int firstGrowthSize;

    byte[] buffer;

    int pos;

    final InternalSerializationService service;

    private final boolean isBigEndian;

    ByteArrayObjectDataOutput(int size, InternalSerializationService service, ByteOrder byteOrder) {
        this(size, -1, service, byteOrder);
    }

    ByteArrayObjectDataOutput(int initialSize, int firstGrowthSize, InternalSerializationService service, ByteOrder byteOrder) {
        this.initialSize = initialSize;
        this.firstGrowthSize = firstGrowthSize;
        this.buffer = new byte[initialSize];
        this.service = service;
        isBigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
    }

    @Override
    public void write(int b) {
        ensureAvailable(1);
        buffer[pos++] = (byte) (b);
    }

    @Override
    public void write(int position, int b) {
        buffer[position] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else {
            ArrayUtils.boundsCheck(b.length, off, len);
        }
        if (len == 0) {
            return;
        }
        ensureAvailable(len);
        System.arraycopy(b, off, buffer, pos, len);
        pos += len;
    }

    @Override
    public final void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public final void writeBoolean(int position, final boolean v) throws IOException {
        write(position, v ? 1 : 0);
    }

    @Override
    public void writeBooleanBit(int position, int bitIndex, boolean v) {
        byte b = buffer[position];
        if (v) {
            b = (byte) (b | (1 << bitIndex));
        } else {
            b = (byte) (b & ~(1 << bitIndex));
        }
        buffer[position] = b;
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
    public final void writeByte(int position, final int v) throws IOException {
        write(position, v);
    }

    @Override
    public final void writeBytes(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len);
        for (int i = 0; i < len; i++) {
            buffer[pos++] = (byte) s.charAt(i);
        }
    }

    @Override
    public void writeChar(final int v) throws IOException {
        ensureAvailable(CHAR_SIZE_IN_BYTES);
        Bits.writeChar(buffer, pos, (char) v, isBigEndian);
        pos += CHAR_SIZE_IN_BYTES;
    }

    @Override
    public void writeChar(int position, final int v) throws IOException {
        Bits.writeChar(buffer, position, (char) v, isBigEndian);
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
    public void writeDouble(int position, final double v) throws IOException {
        writeLong(position, Double.doubleToLongBits(v));
    }

    @Override
    public void writeDouble(double v, ByteOrder byteOrder) throws IOException {
        writeLong(Double.doubleToLongBits(v), byteOrder);
    }

    @Override
    public void writeDouble(int position, double v, ByteOrder byteOrder) throws IOException {
        writeLong(position, Double.doubleToLongBits(v), byteOrder);
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeFloat(int position, final float v) throws IOException {
        writeInt(position, Float.floatToIntBits(v));
    }

    @Override
    public void writeFloat(float v, ByteOrder byteOrder) throws IOException {
        writeInt(Float.floatToIntBits(v), byteOrder);
    }

    @Override
    public void writeFloat(int position, float v, ByteOrder byteOrder) throws IOException {
        writeInt(position, Float.floatToIntBits(v), byteOrder);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        Bits.writeInt(buffer, pos, v, isBigEndian);
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(int position, int v) throws IOException {
        Bits.writeInt(buffer, position, v, isBigEndian);
    }

    @Override
    public void writeInt(int v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        Bits.writeInt(buffer, pos, v, byteOrder == ByteOrder.BIG_ENDIAN);
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(int position, int v, ByteOrder byteOrder) throws IOException {
        Bits.writeInt(buffer, position, v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void writeLong(final long v) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        Bits.writeLong(buffer, pos, v, isBigEndian);
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(int position, final long v) throws IOException {
        Bits.writeLong(buffer, position, v, isBigEndian);
    }

    @Override
    public void writeLong(long v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        Bits.writeLong(buffer, pos, v, byteOrder == ByteOrder.BIG_ENDIAN);
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(int position, long v, ByteOrder byteOrder) throws IOException {
        Bits.writeLong(buffer, position, v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void writeShort(final int v) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        Bits.writeShort(buffer, pos, (short) v, isBigEndian);
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(int position, final int v) throws IOException {
        Bits.writeShort(buffer, position, (short) v, isBigEndian);
    }

    @Override
    public void writeShort(int v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        Bits.writeShort(buffer, pos, (short) v, byteOrder == ByteOrder.BIG_ENDIAN);
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(int position, int v, ByteOrder byteOrder) throws IOException {
        Bits.writeShort(buffer, position, (short) v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    @Deprecated
    public void writeUTF(final String str) throws IOException {
        writeString(str);
    }

    @Override
    public void writeString(@Nullable String str) throws IOException {
        if (str == null) {
            writeInt(NULL_ARRAY_LENGTH);
            return;
        }

        byte[] utf8Bytes = str.getBytes(StandardCharsets.UTF_8);
        writeInt(utf8Bytes.length);
        ensureAvailable(utf8Bytes.length);
        write(utf8Bytes);
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
    @Deprecated
    public void writeUTFArray(String[] strings) throws IOException {
       writeStringArray(strings);
    }

    @Override
    public void writeStringArray(@Nullable String[] strings) throws IOException {
        int len = strings != null ? strings.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (String s : strings) {
                writeString(s);
            }
        }
    }

    final void ensureAvailable(int len) {
        if (available() < len) {
            if (buffer != null) {
                int newCap = Math.max(Math.max(buffer.length << 1, buffer.length + len), firstGrowthSize);
                buffer = Arrays.copyOf(buffer, newCap);
            } else {
                buffer = new byte[len > initialSize / 2 ? len * 2 : initialSize];
            }
        }
    }

    @Override
    public void writeObject(Object object) throws IOException {
        service.writeObject(this, object);
    }

    @Override
    public void writeData(Data data) throws IOException {
        int len = data == null ? NULL_ARRAY_LENGTH : data.totalSize();
        writeInt(len);
        if (len > 0) {
            ensureAvailable(len);
            data.copyTo(buffer, pos);
            pos += len;
        }
    }

    /**
     * Returns this buffer's position.
     */
    @Override
    public final int position() {
        return pos;
    }

    @Override
    public void position(int newPos) {
        if ((newPos > buffer.length) || (newPos < 0)) {
            throw new IllegalArgumentException();
        }

        pos = newPos;
    }

    public int available() {
        return buffer != null ? buffer.length - pos : 0;
    }

    @Override
    public byte[] toByteArray() {
        return toByteArray(0);
    }

    @Override
    public byte[] toByteArray(int padding) {
        if (buffer == null || pos == 0) {
            return new byte[padding];
        }

        final byte[] newBuffer = new byte[padding + pos];
        System.arraycopy(buffer, 0, newBuffer, padding, pos);
        return newBuffer;
    }

    @Override
    public void clear() {
        pos = 0;
        if (buffer != null && buffer.length > initialSize * 8) {
            buffer = new byte[initialSize * 8];
        }
        version = UNKNOWN;
        wanProtocolVersion = UNKNOWN;
    }

    @Override
    public void close() {
        pos = 0;
        buffer = null;
    }

    @Override
    public ByteOrder getByteOrder() {
        return isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public SerializationService getSerializationService() {
        return service;
    }

    @Override
    public String toString() {
        return "ByteArrayObjectDataOutput{"
                + "size=" + (buffer != null ? buffer.length : 0)
                + ", pos=" + pos
                + '}';
    }
}
