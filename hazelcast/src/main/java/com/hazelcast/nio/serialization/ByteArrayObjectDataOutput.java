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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.UTFEncoderDecoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

class ByteArrayObjectDataOutput extends OutputStream implements BufferObjectDataOutput {

    final int initialSize;

    byte[] buffer;

    int pos;

    final SerializationService service;

    private byte[] utfBuffer;

    private final boolean isBigEndian;

    ByteArrayObjectDataOutput(int size, SerializationService service, ByteOrder byteOrder) {
        this.initialSize = size;
        this.buffer = new byte[size];
        this.service = service;
        isBigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
    }

    public void write(int b) {
        ensureAvailable(1);
        buffer[pos++] = (byte) (b);
    }

    public void write(int position, int b) {
        buffer[position] = (byte) b;
    }

    public void write(byte[] b, int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        ensureAvailable(len);
        System.arraycopy(b, off, buffer, pos, len);
        pos += len;
    }

    public final void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    public final void writeBoolean(int position, final boolean v) throws IOException {
        write(position, v ? 1 : 0);
    }

    public final void writeByte(final int v) throws IOException {
        write(v);
    }

    public final void writeZeroBytes(int count) {
        for (int k = 0; k < count; k++) {
            write(0);
        }
    }

    public final void writeByte(int position, final int v) throws IOException {
        write(position, v);
    }

    public final void writeBytes(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len);
        for (int i = 0; i < len; i++) {
            buffer[pos++] = (byte) s.charAt(i);
        }
    }

    public void writeChar(final int v) throws IOException {
        ensureAvailable(CHAR_SIZE_IN_BYTES);
        Bits.writeChar(buffer, pos, (char) v, isBigEndian);
        pos += CHAR_SIZE_IN_BYTES;
    }

    public void writeChar(int position, final int v) throws IOException {
        Bits.writeChar(buffer, position, (char) v, isBigEndian);
    }

    public void writeChars(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len * CHAR_SIZE_IN_BYTES);
        for (int i = 0; i < len; i++) {
            final int v = s.charAt(i);
            writeChar(pos, v);
            pos += CHAR_SIZE_IN_BYTES;
        }
    }

    public void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

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

    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

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

    public void writeInt(final int v) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        Bits.writeInt(buffer, pos, v, isBigEndian);
        pos += INT_SIZE_IN_BYTES;
    }

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

    public void writeLong(final long v) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        Bits.writeLong(buffer, pos, v, isBigEndian);
        pos += LONG_SIZE_IN_BYTES;
    }

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

    public void writeShort(final int v) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        Bits.writeShort(buffer, pos, (short) v, isBigEndian);
        pos += SHORT_SIZE_IN_BYTES;
    }

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

    public void writeUTF(final String str) throws IOException {
        if (utfBuffer == null) {
            utfBuffer = new byte[UTF_BUFFER_SIZE];
        }
        UTFEncoderDecoder.writeUTF(this, str, utfBuffer);
    }

    public void writeByteArray(byte[] bytes) throws IOException {
        int len = (bytes == null) ? 0 : bytes.length;
        writeInt(len);
        if (len > 0) {
            write(bytes);
        }
    }

    public void writeCharArray(char[] chars) throws IOException {
        int len = chars != null ? chars.length : 0;
        writeInt(len);
        if (len > 0) {
            for (char c : chars) {
                writeChar(c);
            }
        }
    }

    public void writeIntArray(int[] ints) throws IOException {
        int len = ints != null ? ints.length : 0;
        writeInt(len);
        if (len > 0) {
            for (int i : ints) {
                writeInt(i);
            }
        }
    }

    public void writeLongArray(long[] longs) throws IOException {
        int len = longs != null ? longs.length : 0;
        writeInt(len);
        if (len > 0) {
            for (long l : longs) {
                writeLong(l);
            }
        }
    }

    public void writeDoubleArray(double[] doubles) throws IOException {
        int len = doubles != null ? doubles.length : 0;
        writeInt(len);
        if (len > 0) {
            for (double d : doubles) {
                writeDouble(d);
            }
        }
    }

    public void writeFloatArray(float[] floats) throws IOException {
        int len = floats != null ? floats.length : 0;
        writeInt(len);
        if (len > 0) {
            for (float f : floats) {
                writeFloat(f);
            }
        }
    }

    public void writeShortArray(short[] shorts) throws IOException {
        int len = shorts != null ? shorts.length : 0;
        writeInt(len);
        if (len > 0) {
            for (short s : shorts) {
                writeShort(s);
            }
        }
    }

    final void ensureAvailable(int len) {
        if (available() < len) {
            if (buffer != null) {
                int newCap = Math.max(buffer.length << 1, buffer.length + len);
                byte[] newBuffer = new byte[newCap];
                System.arraycopy(buffer, 0, newBuffer, 0, pos);
                buffer = newBuffer;
            } else {
                buffer = new byte[len > initialSize / 2 ? len * 2 : initialSize];
            }
        }
    }

    public void writeObject(Object object) throws IOException {
        service.writeObject(this, object);
    }

    public void writeData(Data data) throws IOException {
        service.writeData(this, data);
    }

    /**
     * Returns this buffer's position.
     */
    public final int position() {
        return pos;
    }

    public void position(int newPos) {
        if ((newPos > buffer.length) || (newPos < 0)) {
            throw new IllegalArgumentException();
        }

        pos = newPos;
    }

    public int available() {
        return buffer != null ? buffer.length - pos : 0;
    }

    public byte toByteArray()[] {
        if (buffer == null || pos == 0) {
            return new byte[0];
        }
        final byte[] newBuffer = new byte[pos];
        System.arraycopy(buffer, 0, newBuffer, 0, pos);
        return newBuffer;
    }

    public void clear() {
        pos = 0;
        if (buffer != null && buffer.length > initialSize * 8) {
            buffer = new byte[initialSize * 8];
        }
    }

    public void close() {
        pos = 0;
        buffer = null;
    }

    public ByteOrder getByteOrder() {
        return isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ByteArrayObjectDataOutput");
        sb.append("{size=").append(buffer != null ? buffer.length : 0);
        sb.append(", pos=").append(pos);
        sb.append('}');
        return sb.toString();
    }
}
