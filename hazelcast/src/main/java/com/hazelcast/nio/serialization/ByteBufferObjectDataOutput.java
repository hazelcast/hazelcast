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
import com.hazelcast.nio.DynamicByteBuffer;
import com.hazelcast.nio.UTFEncoderDecoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class ByteBufferObjectDataOutput extends OutputStream implements BufferObjectDataOutput, PortableContextAware {

    private static final int UTF_BUFFER_SIZE = 1024;

    private final DynamicByteBuffer buffer;

    private final SerializationService service;

    private byte[] utfBuffer;

    ByteBufferObjectDataOutput(int size, SerializationService service, ByteOrder order) {
        this.buffer = new DynamicByteBuffer(size, false);
        this.service = service;
        this.buffer.order(order);
    }

    ByteBufferObjectDataOutput(ByteBuffer buffer, SerializationService service, ByteOrder order) {
        this.buffer = new DynamicByteBuffer(buffer);
        buffer.order(order);
        this.service = service;
    }

    public void write(int b) {
        buffer.put((byte) b);
    }

    public void write(int position, int b) {
        buffer.put(position, (byte) b);
    }

    public void write(byte[] b, int off, int len) {
        if (len == 0) {
            return;
        }
        buffer.put(b, off, len);
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
        for (int i = 0; i < len; i++) {
            buffer.put((byte) s.charAt(i));
        }
    }

    public void writeChar(final int v) throws IOException {
        buffer.putChar((char) v);
    }

    public void writeChar(int position, final int v) throws IOException {
        buffer.putChar(position, (char) v);
    }

    public void writeChars(final String s) throws IOException {
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            writeChar(s.charAt(i));
        }
    }

    public void writeDouble(final double v) throws IOException {
        buffer.putDouble(v);
    }

    public void writeDouble(int position, final double v) throws IOException {
        buffer.putDouble(position, v);
    }

    public void writeFloat(final float v) throws IOException {
        buffer.putFloat(v);
    }

    public void writeFloat(int position, final float v) throws IOException {
        buffer.putFloat(position, v);
    }

    public void writeInt(final int v) throws IOException {
        buffer.putInt(v);
    }

    public void writeInt(int position, int v) throws IOException {
        buffer.putInt(position, v);
    }

    public void writeLong(final long v) throws IOException {
        buffer.putLong(v);
    }

    public void writeLong(int position, final long v) throws IOException {
        buffer.putLong(position, v);
    }

    public void writeShort(final int v) throws IOException {
        buffer.putShort((short) v);
    }

    public void writeShort(int position, final int v) throws IOException {
        buffer.putShort(position, (short) v);
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

    public void writeUTF(final String str) throws IOException {
        if (utfBuffer == null) {
            utfBuffer = new byte[UTF_BUFFER_SIZE];
        }
        UTFEncoderDecoder.writeUTF(this, str, utfBuffer);
    }

    public void writeObject(Object object) throws IOException {
        service.writeObject(this, object);
    }

    /**
     * Returns this buffer's position.
     */
    public int position() {
        return buffer.position();
    }

    public void position(int newPos) {
        buffer.position(newPos);
    }

    public int available() {
        return buffer != null ? buffer.remaining() : 0;
    }

    public byte[] getBuffer() {
        return buffer.array();
    }

    public byte toByteArray()[] {
        if (buffer == null) {
            return new byte[0];
        }
        final DynamicByteBuffer duplicate = buffer.duplicate();
        duplicate.flip();
        final byte[] newBuffer = new byte[duplicate.limit()];
        duplicate.get(newBuffer);
        return newBuffer;
    }

    public void clear() {
        buffer.clear();
    }

    public void close() {
        buffer.close();
    }

    public PortableContext getPortableContext() {
        return service.getPortableContext();
    }

    public ByteOrder getByteOrder() {
        return buffer.order();
    }

    @Override
    public SerializationService getSerializationService() {
        return service;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ByteBufferObjectDataOutput{");
        sb.append("buffer=").append(buffer);
        sb.append('}');
        return sb.toString();
    }
}
