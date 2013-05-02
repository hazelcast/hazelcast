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
import com.hazelcast.nio.UTFUtil;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
* @mdogan 12/26/12
*/
class ContextAwareDataInput extends InputStream implements BufferObjectDataInput, SerializationContextAware {

    private final byte buffer[];

    private final int size;

    private final int offset;

    private final byte longBuffer[] = new byte[8];

    private int pos = 0;

    private int mark = 0;

    private final SerializationService service;

    private int factoryId;

    private int dataClassId;

    private int dataVersion;

    public ContextAwareDataInput(byte[] buffer, SerializationService service) {
        this(buffer, 0, service);
    }

    public ContextAwareDataInput(Data data, SerializationService service) {
        this(data.buffer, 0, service);
        final ClassDefinition cd = data.classDefinition;
        this.factoryId = cd != null ? cd.getFactoryId() : 0;
        this.dataClassId = cd != null ? cd.getClassId() : -1;
        this.dataVersion = cd != null ? cd.getVersion() : -1;
    }

    private ContextAwareDataInput(byte buffer[], int offset, SerializationService service) {
        super();
        this.buffer = buffer;
        this.size = buffer.length - offset;
        this.offset = offset;
        this.service = service;
    }

    @Override
    public int read() throws IOException {
        return (pos < size) ? (buffer[offset + pos++] & 0xff) : -1;
    }

    public int read(int index) throws IOException {
        return (index < size) ? (buffer[offset + index] & 0xff) : -1;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        final int read = read(pos, b, off, len);
        pos += read;
        return read;
    }

    public int read(int index, byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (index >= size) {
            return -1;
        }
        if (index + len > size) {
            len = size - index;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buffer, offset + index, b, off, len);
        return len;
    }

    public boolean readBoolean() throws IOException {
        final int ch = read();
        if (ch < 0)
            throw new EOFException();
        return (ch != 0);
    }

    public boolean readBoolean(int index) throws IOException {
        final int ch = read(index);
        if (ch < 0)
            throw new EOFException();
        return (ch != 0);
    }

    /**
     * See the general contract of the <code>readByte</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream as a signed 8-bit
     *         <code>byte</code>.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public byte readByte() throws IOException {
        final int ch = read();
        if (ch < 0)
            throw new EOFException();
        return (byte) (ch);
    }

    public byte readByte(int index) throws IOException {
        final int ch = read(index);
        if (ch < 0)
            throw new EOFException();
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
     *                      bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public char readChar() throws IOException {
        final char c = readChar(pos);
        pos += 2;
        return c;
    }

    public char readChar(int index) throws IOException {
        final int ch1 = read(index);
        final int ch2 = read(index + 1);
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char) ((ch1 << 8) + (ch2 << 0));
    }

    /**
     * See the general contract of the <code>readDouble</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a
     *         <code>double</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading eight
     *                      bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.DataInputStream#readLong()
     * @see Double#longBitsToDouble(long)
     */
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public double readDouble(int index) throws IOException {
        return Double.longBitsToDouble(readLong(index));
    }

    /**
     * See the general contract of the <code>readFloat</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as a
     *         <code>float</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading four
     *                      bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.DataInputStream#readInt()
     * @see Float#intBitsToFloat(int)
     */
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public float readFloat(int index) throws IOException {
        return Float.intBitsToFloat(readInt(index));
    }

    public void readFully(final byte b[]) throws IOException {
        read(b);
    }

    public void readFully(final byte b[], final int off, final int len) throws IOException {
        read(b, off, len);
    }

    /**
     * See the general contract of the <code>readInt</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as an
     *         <code>int</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading four
     *                      bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public int readInt() throws IOException {
        final int i = readInt(pos);
        pos += 4;
        return i;
    }

    public int readInt(int index) throws IOException {
        final int ch1 = read(index);
        final int ch2 = read(index + 1);
        final int ch3 = read(index + 2);
        final int ch4 = read(index + 3);
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
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
     *             of JDK&nbsp;1.1, the preferred way to read lines of text is
     *             via the <code>BufferedReader.readLine()</code> method.
     *             Programs that use the <code>DataInputStream</code> class to
     *             read lines can be converted to use the
     *             <code>BufferedReader</code> class.
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
     *         <code>long</code>.
     * @throws java.io.EOFException if this input stream reaches the end before reading eight
     *                      bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public long readLong() throws IOException {
        final long l = readLong(pos);
        pos += 8;
        return l;
    }

    public long readLong(int index) throws IOException {
        read(index, longBuffer, 0, 8);
        return (((long) longBuffer[0] << 56) + ((long) (longBuffer[1] & 255) << 48)
                + ((long) (longBuffer[2] & 255) << 40) + ((long) (longBuffer[3] & 255) << 32)
                + ((long) (longBuffer[4] & 255) << 24) + ((longBuffer[5] & 255) << 16)
                + ((longBuffer[6] & 255) << 8) + ((longBuffer[7] & 255) << 0));
    }

    /**
     * See the general contract of the <code>readShort</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as a signed
     *         16-bit number.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                      bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public short readShort() throws IOException {
        final int ch1 = read();
        final int ch2 = read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short) ((ch1 << 8) + (ch2 << 0));
    }

    public short readShort(int index) throws IOException {
        final int ch1 = read(index);
        final int ch2 = read(index + 1);
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short) ((ch1 << 8) + (ch2 << 0));
    }

    /**
     * See the general contract of the <code>readUnsignedByte</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream, interpreted as an unsigned
     *         8-bit number.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public int readUnsignedByte() throws IOException {
        final int ch = read();
        if (ch < 0)
            throw new EOFException();
        return ch;
    }

    /**
     * See the general contract of the <code>readUnsignedShort</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as an
     *         unsigned 16-bit integer.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                      bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public int readUnsignedShort() throws IOException {
        final int ch1 = read();
        final int ch2 = read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (ch1 << 8) + (ch2 << 0);
    }

    /**
     * See the general contract of the <code>readUTF</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return a Unicode string.
     * @throws java.io.EOFException           if this input stream reaches the end before reading all
     *                                the bytes.
     * @throws java.io.IOException            if an I/O error occurs.
     * @throws java.io.UTFDataFormatException if the bytes do not represent a valid modified UTF-8
     *                                encoding of a string.
     * @see java.io.DataInputStream#readUTF(java.io.DataInput)
     */
    public String readUTF() throws IOException {
        return UTFUtil.readUTF(this);
    }

    public Object readObject() throws IOException {
        return service.readObject(this);
    }

    public ContextAwareDataInput duplicate() {
        return new ContextAwareDataInput(buffer, 0, service);
    }

    public ContextAwareDataInput slice() {
        return new ContextAwareDataInput(buffer, pos, service);
    }

    @Override
    public long skip(long n) {
        if (pos + n > size) {
            n = size - pos;
        }
        if (n < 0) {
            return 0;
        }
        pos += n;
        return n;
    }

    public int skipBytes(final int n) throws IOException {
        int total = 0;
        int cur = 0;
        while ((total < n) && ((cur = (int) skip(n - total)) > 0)) {
            total += cur;
        }
        return total;
    }

    /**
     * Returns this buffer's position.
     */
    public int position() {
        return pos;
    }

    public void position(int newPos) {
        if ((newPos > size) || (newPos < 0))
            throw new IllegalArgumentException();
        pos = newPos;
        if (mark > pos) mark = -1;
    }

    @Override
    public int available() {
        return size - pos;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        mark = pos;
    }

    @Override
    public void reset() {
        pos = mark;
    }

    @Override
    public void close() {
        factoryId = 0;
        dataClassId = -1;
        dataVersion = -1;
    }

    public SerializationContext getSerializationContext() {
        return service.getSerializationContext();
    }

    int getFactoryId() {
        return factoryId;
    }

    void setFactoryId(int factoryId) {
        this.factoryId = factoryId;
    }

    int getDataClassId() {
        return dataClassId;
    }

    void setDataClassId(int classId) {
        this.dataClassId = classId;
    }

    int getDataVersion() {
        return dataVersion;
    }

    void setDataVersion(int dataVersion) {
        this.dataVersion = dataVersion;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ContextAwareDataInput");
        sb.append("{size=").append(size);
        sb.append(", offset=").append(offset);
        sb.append(", pos=").append(pos);
        sb.append(", mark=").append(mark);
        sb.append('}');
        return sb.toString();
    }
}
