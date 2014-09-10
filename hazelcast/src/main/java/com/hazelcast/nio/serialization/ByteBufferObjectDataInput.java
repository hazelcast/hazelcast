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

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.DynamicByteBuffer;
import com.hazelcast.nio.UTFEncoderDecoder;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;

final class ByteBufferObjectDataInput
        extends PortableContextAwareInputStream
        implements BufferObjectDataInput, PortableContextAware, SerializationServiceAccessor.SerializationServiceAccess {

    private static final int UTF_BUFFER_SIZE = 1024;

    private final DynamicByteBuffer buffer;

    private final SerializationService service;

    private byte[] utfBuffer;

    ByteBufferObjectDataInput(Data data, SerializationService service, ByteOrder order) {
        this(data.buffer, service, order);
        final ClassDefinition cd = data.classDefinition;
        setClassDefinition(cd);
    }

    ByteBufferObjectDataInput(byte[] buffer, SerializationService service, ByteOrder order) {
        super();
        this.buffer = new DynamicByteBuffer(buffer);
        this.buffer.order(order);
        this.service = service;
    }

    public int read() throws IOException {
        return buffer.hasRemaining() ? buffer.get() : -1;
    }

    public int read(int position) throws IOException {
        return buffer.hasRemaining() ? buffer.get(position) : -1;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (len <= 0) {
            return 0;
        }
        final int pos = buffer.position();
        final int size = buffer.capacity();
        if (pos >= size) {
            return -1;
        }
        if (pos + len > size) {
            len = size - pos;
        }
        buffer.get(b, off, len);
        return len;
    }

    public boolean readBoolean() throws IOException {
        final int ch = readByte();
        return (ch != 0);
    }

    public boolean readBoolean(int position) throws IOException {
        final int ch = readByte(position);
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
    public byte readByte() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    public byte readByte(int position) throws IOException {
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
    public char readChar() throws IOException {
        return buffer.getChar();
    }

    public char readChar(int position) throws IOException {
        return buffer.getChar(position);
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
        return buffer.getDouble();
    }

    public double readDouble(int position) throws IOException {
        return buffer.getDouble(position);
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
        return buffer.getFloat();
    }

    public float readFloat(int position) throws IOException {
        return buffer.getFloat(position);
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
    public int readInt() throws IOException {
        return buffer.getInt();
    }

    public int readInt(int position) throws IOException {
        return buffer.getInt(position);
    }

    /**
     * See the general contract of the <code>readLine</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next line of text from this input stream.
     * @throws java.io.IOException if an I/O error occurs.
     * @see java.io.BufferedReader#readLine()
     * @see java.io.FilterInputStream#in
     * @deprecated This method does not properly convert bytes to characters. As
     * of JDK&nbsp;1.1, the preferred way to read lines of text is
     * via the <code>BufferedReader.readLine()</code> method.
     * Programs that use the <code>DataInputStream</code> class to
     * read lines can be converted to use the
     * <code>BufferedReader</code> class.
     */
    @Deprecated
    public String readLine() throws IOException {
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
    public long readLong() throws IOException {
        return buffer.getLong();
    }

    public long readLong(int position) throws IOException {
        return buffer.getLong(position);
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
    public short readShort() throws IOException {
        return buffer.getShort();
    }

    public short readShort(int position) throws IOException {
        return buffer.getShort(position);
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
        return buffer.getShort();
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
    public String readUTF() throws IOException {
        if (utfBuffer == null) {
            utfBuffer = new byte[UTF_BUFFER_SIZE];
        }
        return UTFEncoderDecoder.readUTF(this, utfBuffer);
    }

    public Object readObject() throws IOException {
        return service.readObject(this);
    }

    @Override
    public long skip(long n) {
        if (n <= 0 || n >= Integer.MAX_VALUE) {
            return 0L;
        }
        return skipBytes((int) n);
    }

    public int skipBytes(final int n) {
        if (n <= 0) {
            return 0;
        }
        int skip = n;
        final int pos = position();
        final int cap = buffer.capacity();
        if (pos + skip > cap) {
            skip = cap - pos;
        }
        position(pos + skip);
        return skip;
    }

    public byte[] getBuffer() {
        return buffer.array();
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

    @Override
    public int available() {
        return buffer.remaining();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        buffer.mark();
    }

    @Override
    public void reset() {
        buffer.reset();
    }

    @Override
    public void close() {
        buffer.close();
    }

    public PortableContext getPortableContext() {
        return service.getPortableContext();
    }

    @Override
    public ClassLoader getClassLoader() {
        return service.getClassLoader();
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
        final StringBuilder sb = new StringBuilder("ByteBufferObjectDataInput{");
        sb.append("buffer=").append(buffer);
        sb.append('}');
        return sb.toString();
    }
}
