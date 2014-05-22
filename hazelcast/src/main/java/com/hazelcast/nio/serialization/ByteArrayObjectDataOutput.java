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

import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.UTFEncoderDecoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;

class ByteArrayObjectDataOutput extends OutputStream implements BufferObjectDataOutput, PortableContextAware {

    private static final int UTF_BUFFER_SIZE = 1024;

    protected final int initialSize;

    private final SerializationService serializationService;

    protected byte[] buffer;

    protected int pos;

    private byte[] utfBuffer;

    ByteArrayObjectDataOutput(int size, SerializationService serializationService) {
        this.initialSize = size;
        this.buffer = new byte[size];
        this.serializationService = serializationService;
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

    public void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    public void writeBoolean(int position, final boolean v) throws IOException {
        write(position, v ? 1 : 0);
    }

    public void writeByte(final int v) throws IOException {
        write(v);
    }

    public void writeZeroBytes(int count) {
        for (int k = 0; k < count; k++) {
            write(0);
        }
    }

    public void writeByte(int position, final int v) throws IOException {
        write(position, v);
    }

    public void writeBytes(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len);
        for (int i = 0; i < len; i++) {
            buffer[pos++] = (byte) s.charAt(i);
        }
    }

    public void writeChar(final int v) throws IOException {
        ensureAvailable(2);
        buffer[pos++] = (byte) ((v >>> 8) & 0xFF);
        buffer[pos++] = (byte) ((v) & 0xFF);
    }

    public void writeChar(int position, final int v) throws IOException {
        write(position, (v >>> 8) & 0xFF);
        write(position + 1, (v) & 0xFF);
    }

    public void writeChars(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len * 2);
        for (int i = 0; i < len; i++) {
            final int v = s.charAt(i);
            writeChar(pos, v);
            pos += 2;
        }
    }

    public void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeDouble(int position, final double v) throws IOException {
        writeLong(position, Double.doubleToLongBits(v));
    }

    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeFloat(int position, final float v) throws IOException {
        writeInt(position, Float.floatToIntBits(v));
    }

    public void writeInt(final int v) throws IOException {
        ensureAvailable(4);
        buffer[pos++] = (byte) ((v >>> 24) & 0xFF);
        buffer[pos++] = (byte) ((v >>> 16) & 0xFF);
        buffer[pos++] = (byte) ((v >>> 8) & 0xFF);
        buffer[pos++] = (byte) ((v) & 0xFF);
    }

    public void writeInt(int position, int v) throws IOException {
        write(position, (v >>> 24) & 0xFF);
        write(position + 1, (v >>> 16) & 0xFF);
        write(position + 2, (v >>> 8) & 0xFF);
        write(position + 3, (v) & 0xFF);
    }

    public void writeLong(final long v) throws IOException {
        ensureAvailable(8);
        buffer[pos++] = (byte) (v >>> 56);
        buffer[pos++] = (byte) (v >>> 48);
        buffer[pos++] = (byte) (v >>> 40);
        buffer[pos++] = (byte) (v >>> 32);
        buffer[pos++] = (byte) (v >>> 24);
        buffer[pos++] = (byte) (v >>> 16);
        buffer[pos++] = (byte) (v >>> 8);
        buffer[pos++] = (byte) (v);
    }

    public void writeLong(int position, final long v) throws IOException {
        write(position, (int) (v >>> 56));
        write(position + 1, (int) (v >>> 48));
        write(position + 2, (int) (v >>> 40));
        write(position + 3, (int) (v >>> 32));
        write(position + 4, (int) (v >>> 24));
        write(position + 5, (int) (v >>> 16));
        write(position + 6, (int) (v >>> 8));
        write(position + 7, (int) (v));
    }

    public void writeShort(final int v) throws IOException {
        ensureAvailable(2);
        buffer[pos++] = (byte) ((v >>> 8) & 0xFF);
        buffer[pos++] = (byte) ((v) & 0xFF);
    }

    public void writeShort(int position, final int v) throws IOException {
        write(position, (v >>> 8) & 0xFF);
        write(position + 1, (v) & 0xFF);
    }

    public void writeUTF(final String str) throws IOException {
        if (utfBuffer == null) {
            utfBuffer = new byte[UTF_BUFFER_SIZE];
        }
        UTFEncoderDecoder.writeUTF(this, str, utfBuffer);
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
        serializationService.writeObject(this, object);
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

    public byte[] getBuffer() {
        return buffer;
    }

    public byte toByteArray()[] {
        if (buffer == null) {
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
        clear();
        buffer = null;
    }

    public PortableContext getPortableContext() {
        return serializationService.getPortableContext();
    }

    public ByteOrder getByteOrder() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public SerializationService getSerializationService() {
        return serializationService;
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
