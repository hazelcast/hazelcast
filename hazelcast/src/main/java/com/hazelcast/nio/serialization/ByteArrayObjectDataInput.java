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

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.UTFEncoderDecoder;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

class ByteArrayObjectDataInput extends InputStream implements BufferObjectDataInput, PortableDataInput {

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    ByteBuffer header;

    byte[] data;

    final int size;

    int pos;

    int mark;

    final SerializationService service;

    private byte[] utfBuffer;

    private final boolean bigEndian;

    ByteArrayObjectDataInput(Data data, SerializationService service, ByteOrder byteOrder) {
        this(data.getData(), data.getHeader(), service, byteOrder);
    }

    ByteArrayObjectDataInput(byte[] data, SerializationService service, ByteOrder byteOrder) {
        this(data, null, service, byteOrder);
    }

    private ByteArrayObjectDataInput(byte[] data, byte[] header, SerializationService service, ByteOrder byteOrder) {
        super();
        this.data = data;
        this.size = data != null ? data.length : 0;
        this.service = service;
        bigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
        this.header = header != null ? ByteBuffer.wrap(header).asReadOnlyBuffer().order(byteOrder) : null;
    }

    public int read() throws IOException {
        return (pos < size) ? (data[pos++] & 0xff) : -1;
    }

    public int read(int position) throws IOException {
        return (position < size) ? (data[position] & 0xff) : -1;
    }

    public final int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (len <= 0) {
            return 0;
        }
        if (pos >= size) {
            return -1;
        }
        if (pos + len > size) {
            len = size - pos;
        }
        System.arraycopy(data, pos, b, off, len);
        pos += len;
        return len;
    }

    public final boolean readBoolean() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    public final boolean readBoolean(int position) throws IOException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    /**
     * See the general contract of the <code>readByte</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream as a signed 8-bit
     * <code>byte</code>.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final byte readByte() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    public final byte readByte(int position) throws IOException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    /**
     * See the general contract of the <code>readChar</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream as a Unicode character.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final char readChar() throws IOException {
        final char c = readChar(pos);
        pos += CHAR_SIZE_IN_BYTES;
        return c;
    }

    public char readChar(int position) throws IOException {
        checkAvailable(position, CHAR_SIZE_IN_BYTES);
        return Bits.readChar(data, position, bigEndian);
    }

    /**
     * See the general contract of the <code>readDouble</code> method of
     * <code>DataInput</code>.
     * <p/>
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
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public double readDouble(int position) throws IOException {
        return Double.longBitsToDouble(readLong(position));
    }

    /**
     * See the general contract of the <code>readFloat</code> method of
     * <code>DataInput</code>.
     * <p/>
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
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public float readFloat(int position) throws IOException {
        return Float.intBitsToFloat(readInt(position));
    }

    public void readFully(final byte[] b) throws IOException {
        if (read(b) == -1) {
            throw new EOFException("End of stream reached");
        }
    }

    public void readFully(final byte[] b, final int off, final int len) throws IOException {
        if (read(b, off, len) == -1) {
            throw new EOFException("End of stream reached");
        }
    }

    /**
     * See the general contract of the <code>readInt</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as an
     * <code>int</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading four
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final int readInt() throws IOException {
        final int i = readInt(pos);
        pos += INT_SIZE_IN_BYTES;
        return i;
    }

    public int readInt(int position) throws IOException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return Bits.readInt(data, position, bigEndian);
    }

    @Deprecated
    public final String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * See the general contract of the <code>readLong</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a
     * <code>long</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading eight
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final long readLong() throws IOException {
        final long l = readLong(pos);
        pos += LONG_SIZE_IN_BYTES;
        return l;
    }

    public long readLong(int position) throws IOException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return Bits.readLong(data, position, bigEndian);
    }

    /**
     * See the general contract of the <code>readShort</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as a signed
     * 16-bit number.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final short readShort() throws IOException {
        short s = readShort(pos);
        pos += SHORT_SIZE_IN_BYTES;
        return s;
    }

    public short readShort(int position) throws IOException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        return Bits.readShort(data, position, bigEndian);
    }

    public byte[] readByteArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            byte[] b = new byte[len];
            readFully(b);
            return b;
        }
        return new byte[0];
    }

    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            char[] values = new char[len];
            for (int i = 0; i < len; i++) {
                values[i] = readChar();
            }
            return values;
        }
        return new char[0];
    }

    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            int[] values = new int[len];
            for (int i = 0; i < len; i++) {
                values[i] = readInt();
            }
            return values;
        }
        return new int[0];
    }

    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            long[] values = new long[len];
            for (int i = 0; i < len; i++) {
                values[i] = readLong();
            }
            return values;
        }
        return new long[0];
    }

    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            double[] values = new double[len];
            for (int i = 0; i < len; i++) {
                values[i] = readDouble();
            }
            return values;
        }
        return new double[0];
    }

    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            float[] values = new float[len];
            for (int i = 0; i < len; i++) {
                values[i] = readFloat();
            }
            return values;
        }
        return new float[0];
    }

    public short[] readShortArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            short[] values = new short[len];
            for (int i = 0; i < len; i++) {
                values[i] = readShort();
            }
            return values;
        }
        return new short[0];
    }

    /**
     * See the general contract of the <code>readUnsignedByte</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream, interpreted as an unsigned
     * 8-bit number.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public int readUnsignedByte() throws IOException {
        return readByte();
    }

    /**
     * See the general contract of the <code>readUnsignedShort</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as an
     * unsigned 16-bit integer.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public int readUnsignedShort() throws IOException {
        return readShort();
    }

    /**
     * See the general contract of the <code>readUTF</code> method of
     * <code>DataInput</code>.
     * <p/>
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
    public final String readUTF() throws IOException {
        if (utfBuffer == null) {
            utfBuffer = new byte[UTF_BUFFER_SIZE];
        }
        return UTFEncoderDecoder.readUTF(this, utfBuffer);
    }

    @Override
    public final Object readObject() throws IOException {
        return service.readObject(this);
    }

    @Override
    public final Data readData() throws IOException {
        return service.readData(this);
    }

    @Override
    public final long skip(long n) {
        if (n <= 0 || n >= Integer.MAX_VALUE) {
            return 0L;
        }
        return skipBytes((int) n);
    }

    @Override
    public final int skipBytes(final int n) {
        if (n <= 0) {
            return 0;
        }
        int skip = n;
        final int pos = position();
        if (pos + skip > size) {
            skip = size - pos;
        }
        position(pos + skip);
        return skip;
    }

    /**
     * Returns this buffer's position.
     */
    @Override
    public final int position() {
        return pos;
    }

    @Override
    public final void position(int newPos) {
        if ((newPos > size) || (newPos < 0)) {
            throw new IllegalArgumentException();
        }
        pos = newPos;
        if (mark > pos) {
            mark = -1;
        }
    }

    final void checkAvailable(int pos, int k) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Negative pos! -> " + pos);
        }
        if ((size - pos) < k) {
            throw new EOFException("Cannot read " + k + " bytes!");
        }
    }

    @Override
    public final int available() {
        return size - pos;
    }

    @Override
    public final boolean markSupported() {
        return true;
    }

    @Override
    public final void mark(int readlimit) {
        mark = pos;
    }

    @Override
    public final void reset() {
        pos = mark;
    }

    @Override
    public final void close() {
        header = null;
        data = null;
        utfBuffer = null;
    }

    @Override
    public final ClassLoader getClassLoader() {
        return service.getClassLoader();
    }

    public ByteOrder getByteOrder() {
        return bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public ByteBuffer getHeaderBuffer() {
        return header != null ? header : EMPTY_BUFFER;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ByteArrayObjectDataInput");
        sb.append("{size=").append(size);
        sb.append(", pos=").append(pos);
        sb.append(", mark=").append(mark);
        sb.append('}');
        return sb.toString();
    }
}
