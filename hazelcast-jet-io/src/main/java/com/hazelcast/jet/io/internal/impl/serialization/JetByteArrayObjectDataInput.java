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

package com.hazelcast.jet.io.internal.impl.serialization;

import com.hazelcast.jet.io.api.serialization.JetDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
 * Provides methods which let to de-serialize data directly from some byte-represented part of memory
 * <p>
 * It consumes pointer and address and lets to read objects from the byte-represented part of memory
 * It doesn't release memory, memory-releasing is responsibility of the external environment
 */
@SuppressWarnings({
        "checkstyle:methodcount",
        "checkstyle:magicnumber"
})
public class JetByteArrayObjectDataInput implements JetDataInput, ObjectDataInput {

    long dataAddress;
    long size;
    long pos;

    final InternalSerializationService service;

    char[] charBuffer;

    private final boolean bigEndian;

    private MemoryManager memoryManager;

    JetByteArrayObjectDataInput(MemoryManager memoryManager,
                                InternalSerializationService service,
                                ByteOrder byteOrder) {
        this.service = service;
        this.pos = 0;
        this.bigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
        this.memoryManager = memoryManager;
    }

    @Override
    public void reset(long pointer, long size) {
        this.pos = 0;
        this.size = size;
        this.dataAddress = pointer;

        if (charBuffer != null && charBuffer.length > BufferObjectDataInput.UTF_BUFFER_SIZE * 8) {
            Arrays.fill(charBuffer, (char) 0);
        }
    }

    @Override
    public void setMemoryManager(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    @Override
    public void clear() {
        pos = 0;
        size = 0;
        dataAddress = 0;

        if (charBuffer != null && charBuffer.length > BufferObjectDataInput.UTF_BUFFER_SIZE * 8) {
            charBuffer = new char[BufferObjectDataInput.UTF_BUFFER_SIZE * 8];
        }
    }

    @Override
    public int read() throws IOException {
        return (pos < size) ? (memoryManager.getAccessor().getByte(dataAddress + pos++) & 0xff) : -1;
    }

    @Override
    public int read(long position) throws IOException {
        return (position < size) ? (memoryManager.getAccessor().getByte(dataAddress + position) & 0xff) : -1;
    }

    @Override
    public final boolean readBoolean() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    @Override
    public final boolean readBoolean(long position) throws IOException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
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
    public final byte readByte() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    @Override
    public final byte readByte(long position) throws IOException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
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
    public final char readChar() throws IOException {
        final char c = readChar(pos);
        pos += CHAR_SIZE_IN_BYTES;
        return c;
    }

    @Override
    public char readChar(long position) throws IOException {
        checkAvailable(position, CHAR_SIZE_IN_BYTES);
        return EndiannessUtil.readChar(EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                dataAddress + position, bigEndian);
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

    @Override
    public double readDouble(int position) throws IOException {
        return Double.longBitsToDouble(readLong(position));
    }

    @Override
    public double readDouble(ByteOrder byteOrder) throws IOException {
        return Double.longBitsToDouble(readLong(byteOrder));
    }

    @Override
    public double readDouble(long position, ByteOrder byteOrder) throws IOException {
        return Double.longBitsToDouble(readLong(position, byteOrder));
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

    @Override
    public float readFloat(int position) throws IOException {
        return Float.intBitsToFloat(readInt(position));
    }

    @Override
    public float readFloat(ByteOrder byteOrder) throws IOException {
        return Float.intBitsToFloat(readInt(byteOrder));
    }

    @Override
    public float readFloat(int position, ByteOrder byteOrder) throws IOException {
        return Float.intBitsToFloat(readInt(position, byteOrder));
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
    public final int readInt() throws IOException {
        final int i = readInt(pos);
        pos += INT_SIZE_IN_BYTES;
        return i;
    }

    public int readInt(long position) throws IOException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return EndiannessUtil.readInt(EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                dataAddress + position, bigEndian);
    }

    @Override
    public final int readInt(ByteOrder byteOrder) throws IOException {
        final int i = readInt(pos, byteOrder);
        pos += INT_SIZE_IN_BYTES;
        return i;
    }

    @Override
    public int readInt(long position, ByteOrder byteOrder) throws IOException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return EndiannessUtil.readInt(EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                dataAddress + position, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Deprecated
    public final String readLine() throws IOException {
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
    public final long readLong() throws IOException {
        final long l = readLong(pos);
        pos += LONG_SIZE_IN_BYTES;
        return l;
    }

    public long readLong(long position) throws IOException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return EndiannessUtil.readLong(EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                dataAddress + position, bigEndian);
    }

    @Override
    public final long readLong(ByteOrder byteOrder) throws IOException {
        final long l = readLong(pos, byteOrder);
        pos += LONG_SIZE_IN_BYTES;
        return l;
    }

    @Override
    public long readLong(long position, ByteOrder byteOrder) throws IOException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return EndiannessUtil.readLong(EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                dataAddress + position, byteOrder == ByteOrder.BIG_ENDIAN);
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
    public final short readShort() throws IOException {
        short s = readShort(pos);
        pos += SHORT_SIZE_IN_BYTES;
        return s;
    }

    @Override
    public short readShort(long position) throws IOException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        return EndiannessUtil.readShort(EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                dataAddress + position, bigEndian);
    }

    @Override
    public final short readShort(ByteOrder byteOrder) throws IOException {
        short s = readShort(pos, byteOrder);
        pos += SHORT_SIZE_IN_BYTES;
        return s;
    }

    @Override
    public short readShort(long position, ByteOrder byteOrder) throws IOException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        return EndiannessUtil.readShort(EndiannessUtil.CUSTOM_ACCESS, memoryManager.getAccessor(),
                dataAddress + position, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        memoryManager.getAccessor().copyToByteArray(dataAddress + pos, b, off, len);
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
        return readByte() & 0xFF;
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
        return readShort() & 0xffff;
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
    public final String readUTF() throws IOException {
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
            if (b < 0) {
                charBuffer[i] = EndiannessUtil.readUtf8Char(this, b);
            } else {
                charBuffer[i] = (char) b;
            }
        }
        return new String(charBuffer, 0, charCount);
    }

    @Override
    public final <T> T readObject() throws IOException {
        return service.readObject(this);
    }

    @Override
    public final Data readData() throws IOException {
        byte[] bytes = readByteArray();
        Data data = bytes == null ? null : new HeapData(bytes);
        return data;
    }

    /**
     * Returns this buffer's position.
     */
    @Override
    public final long position() {
        return pos;
    }

    @Override
    public final void position(long newPos) {
        if ((newPos >= size) || (newPos < 0)) {
            throw new IllegalArgumentException();
        }
        pos = newPos;
    }

    final void checkAvailable(long pos, long k) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Negative pos! -> " + pos);
        }
        if ((size - pos) < k) {
            throw new EOFException("Cannot read " + k + " bytes!");
        }
    }

    @Override
    public final long available() {
        return size - pos;
    }

    @Override
    public final void close() {
        dataAddress = 0;
        pos = 0;
    }

    @Override
    public final ClassLoader getClassLoader() {
        return service.getClassLoader();
    }

    public ByteOrder getByteOrder() {
        return bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString() {
        return "JetByteArrayObjectDataInput{"
                + "size=" + size
                + ", pos=" + pos
                + '}';
    }
}
