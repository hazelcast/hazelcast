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
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.CUSTOM_ACCESS;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.nio.BufferObjectDataInput.UTF_BUFFER_SIZE;

/**
 * Specialization of {@link ObjectDataInput} backed by a {@link MemoryManager}-provided memory block,
 * characterized by its base address and size.
 */
@SuppressWarnings("checkstyle:methodcount")
public final class MemoryDataInput implements ObjectDataInput {

    public static final int INITIAL_UTF8_BUFSIZE = UTF_BUFFER_SIZE * 8;
    private final boolean isBigEndian;
    private final InternalSerializationService service;
    private final SerializationOptimizer optimizer;
    private long bufBase;
    private long size;
    private long pos;
    private char[] charBuffer;
    private MemoryAccessor accessor;

    public MemoryDataInput(MemoryManager memoryManager, SerializationOptimizer optimizer, boolean isBigEndian) {
        this.service = new DefaultSerializationServiceBuilder().build();
        this.optimizer = optimizer;
        this.pos = 0;
        this.isBigEndian = isBigEndian;
        setMemoryManager(memoryManager);
    }

    public void setMemoryManager(MemoryManager memoryManager) {
        this.accessor = memoryManager != null ? memoryManager.getAccessor() : null;
    }

    public <T> T readOptimized() throws IOException {
        return (T) optimizer.read(this);
    }


    // ObjectDataInput implementation

    @Override
    public <T> T readObject() throws IOException {
        return service.readObject(this);
    }

    public <T> T readDataAsObject() throws IOException {
        Data data = readData();
        return data == null ? null : (T) service.toObject(data);
    }

    @Override
    public <T> T readObject(Class aClass) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean readBoolean() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return ch != 0;
    }


    /**
     * See the general contract of the <code>readByte</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream as a signed 8-bit
     * <code>byte</code>.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public byte readByte() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) ch;
    }

    /**
     * See the general contract of the <code>readChar</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream as a Unicode character.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public char readChar() throws IOException {
        checkAvailable(pos, CHAR_SIZE_IN_BYTES);
        final char c = EndiannessUtil.readChar(CUSTOM_ACCESS, accessor, bufBase + pos, isBigEndian);
        pos += CHAR_SIZE_IN_BYTES;
        return c;
    }

    /**
     * See the general contract of the <code>readDouble</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a
     * <code>double</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading eight
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.DataInputStream#readLong()
     * @see Double#longBitsToDouble(long)
     */
    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * See the general contract of the <code>readFloat</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as a
     * <code>float</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading four
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.DataInputStream#readInt()
     * @see Float#intBitsToFloat(int)
     */
    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    /**
     * See the general contract of the <code>readInt</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as an
     * <code>int</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading four
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public int readInt() throws IOException {
        final int i = readInt(pos);
        pos += INT_SIZE_IN_BYTES;
        return i;
    }

    public int readInt(long position) throws IOException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return EndiannessUtil.readInt(CUSTOM_ACCESS, accessor, bufBase + position, isBigEndian);
    }

    @Override
    @Deprecated
    public String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * See the general contract of the <code>readLong</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a
     * <code>long</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading eight
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public long readLong() throws IOException {
        final long l = readLong(pos);
        pos += LONG_SIZE_IN_BYTES;
        return l;
    }

    public long readLong(long position) throws IOException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return EndiannessUtil.readLong(CUSTOM_ACCESS, accessor, bufBase + position, isBigEndian);
    }

    /**
     * See the general contract of the <code>readShort</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as a signed
     * 16-bit number.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public short readShort() throws IOException {
        checkAvailable(pos, SHORT_SIZE_IN_BYTES);
        short s = EndiannessUtil.readShort(CUSTOM_ACCESS, accessor, bufBase + pos, isBigEndian);
        pos += SHORT_SIZE_IN_BYTES;
        return s;
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        accessor.copyToByteArray(bufBase + pos, b, off, len);
        pos += len;
    }

    @Override
    public int skipBytes(int n) throws IOException {
        pos += n;
        return n;
    }

    @Override
    public byte[] readByteArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            byte[] b = new byte[len];
            readFully(b);
            return b;
        }
        return new byte[0];
    }

    @Override
    public boolean[] readBooleanArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            boolean[] values = new boolean[len];
            for (int i = 0; i < len; i++) {
                values[i] = readBoolean();
            }
            return values;
        }
        return new boolean[0];
    }

    @Override
    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            char[] values = new char[len];
            for (int i = 0; i < len; i++) {
                values[i] = readChar();
            }
            return values;
        }
        return new char[0];
    }

    @Override
    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            int[] values = new int[len];
            for (int i = 0; i < len; i++) {
                values[i] = readInt();
            }
            return values;
        }
        return new int[0];
    }

    @Override
    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            long[] values = new long[len];
            for (int i = 0; i < len; i++) {
                values[i] = readLong();
            }
            return values;
        }
        return new long[0];
    }

    @Override
    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            double[] values = new double[len];
            for (int i = 0; i < len; i++) {
                values[i] = readDouble();
            }
            return values;
        }
        return new double[0];
    }

    @Override
    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            float[] values = new float[len];
            for (int i = 0; i < len; i++) {
                values[i] = readFloat();
            }
            return values;
        }
        return new float[0];
    }

    @Override
    public short[] readShortArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            short[] values = new short[len];
            for (int i = 0; i < len; i++) {
                values[i] = readShort();
            }
            return values;
        }
        return new short[0];
    }

    @Override
    public String[] readUTFArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            String[] values = new String[len];
            for (int i = 0; i < len; i++) {
                values[i] = readUTF();
            }
            return values;
        }
        return new String[0];
    }

    /**
     * See the general contract of the <code>readUnsignedByte</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream, interpreted as an unsigned
     * 8-bit number.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public int readUnsignedByte() throws IOException {
        return Byte.toUnsignedInt(readByte());
    }

    /**
     * See the general contract of the <code>readUnsignedShort</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as an
     * unsigned 16-bit integer.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public int readUnsignedShort() throws IOException {
        return Short.toUnsignedInt(readShort());
    }

    /**
     * See the general contract of the <code>readUTF</code> method of
     * <code>DataInput</code>.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return a Unicode string.
     * @throws java.io.EOFException           if this input stream reaches the end before reading all
     *                                        the bytes.
     * @throws java.io.IOException            if an I/O error occurs.
     * @throws java.io.UTFDataFormatException if the bytes do not represent a valid modified UTF-8
     *                                        encoding of a string.
     * @see java.io.DataInputStream#readUTF(java.io.DataInput)
     */
    @Override
    public String readUTF() throws IOException {
        int charCount = readInt();
        if (charCount == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (charBuffer == null || charCount > charBuffer.length) {
            charBuffer = new char[charCount];
        }
        byte b;
        for (int i = 0; i < charCount; i++) {
            b = readByte();
            charBuffer[i] = b < 0 ? EndiannessUtil.readUtf8Char(this, b) : (char) b;
        }
        return new String(charBuffer, 0, charCount);
    }

    @Override
    public Data readData() throws IOException {
        byte[] bytes = readByteArray();
        Data data = bytes == null ? null : new HeapData(bytes);
        return data;
    }

    @Override
    public ClassLoader getClassLoader() {
        return service.getClassLoader();
    }

    @Override
    public ByteOrder getByteOrder() {
        return isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString() {
        return "JetByteArrayObjectDataInput{size=" + size + ", pos=" + pos + '}';
    }




    // Custom methods


    public int read() {
        return pos < size ? Byte.toUnsignedInt(accessor.getByte(bufBase + pos++)) : -1;
    }

    public long position() {
        return pos;
    }

    public long available() {
        return size - pos;
    }

    public void reset(long address, long size) {
        this.pos = 0;
        this.size = size;
        this.bufBase = address;
    }

    public void clear() {
        pos = 0;
        size = 0;
        bufBase = 0;
        if (charBuffer != null && charBuffer.length > INITIAL_UTF8_BUFSIZE) {
            charBuffer = null;
        }
    }

    private void checkAvailable(long pos, long k) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Negative pos! -> " + pos);
        }
        if (size - pos < k) {
            throw new EOFException("Cannot read " + k + " bytes!");
        }
    }
}
