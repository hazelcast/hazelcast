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

package com.hazelcast.nio;

import java.io.*;

public class FastByteArrayInputStream extends ByteArrayInputStream implements DataInput {

    private final byte readBuffer[] = new byte[8];

    private char lineBuffer[];

    public FastByteArrayInputStream(byte buf[]) {
        super(buf);
    }

    public FastByteArrayInputStream(byte buf[], int offset, int length) {
        super(buf, offset, length);
    }

    @Override
    public int read() {
        return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    @Override
    public int read(byte b[], int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (pos >= count) {
            return -1;
        }
        if (pos + len > count) {
            len = count - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    @Override
    public long skip(long n) {
        if (pos + n > count) {
            n = count - pos;
        }
        if (n < 0) {
            return 0;
        }
        pos += n;
        return n;
    }

    public final int skipBytes(final int n) throws IOException {
        int total = 0;
        int cur = 0;
        while ((total < n) && ((cur = (int) skip(n - total)) > 0)) {
            total += cur;
        }
        return total;
    }

    @Override
    public int available() {
        return count - pos;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void reset() {
        pos = mark;
    }

    public void set(byte[] bytes, int size) {
        this.buf = bytes;
        this.pos = 0;
        this.count = size;
        this.mark = 0;
    }

    public final boolean readBoolean() throws IOException {
        final int ch = read();
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
     * @throws EOFException if this input stream has reached the end.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final byte readByte() throws IOException {
        final int ch = read();
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
     * @throws EOFException if this input stream reaches the end before reading two
     *                      bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final char readChar() throws IOException {
        final int ch1 = read();
        final int ch2 = read();
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
     * @throws EOFException if this input stream reaches the end before reading eight
     *                      bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.DataInputStream#readLong()
     * @see java.lang.Double#longBitsToDouble(long)
     */
    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * See the general contract of the <code>readFloat</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as a
     *         <code>float</code>.
     * @throws EOFException if this input stream reaches the end before reading four
     *                      bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.DataInputStream#readInt()
     * @see java.lang.Float#intBitsToFloat(int)
     */
    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public final void readFully(final byte b[]) throws IOException {
        readFully(b, 0, b.length);
    }

    public final void readFully(final byte b[], final int off, final int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        if (b.length - off < len)
            throw new RuntimeException();
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
     * @throws EOFException if this input stream reaches the end before reading four
     *                      bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final int readInt() throws IOException {
        final int ch1 = read();
        final int ch2 = read();
        final int ch3 = read();
        final int ch4 = read();
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
     * @throws IOException if an I/O error occurs.
     * @see java.io.BufferedReader#readLine()
     * @see java.io.FilterInputStream#in
     * @deprecated This method does not properly convert bytes to characters. As
     *             of JDK&nbsp;1.1, the preferred way to read lines of text is
     *             via the <code>BufferedReader.readLine()</code> method.
     *             Programs that use the <code>DataInputStream</code> class to
     *             read lines can be converted to use the
     *             <code>BufferedReader</code> class by replacing code of the
     *             form: <blockquote>
     *             <p/>
     *             <pre>
     *                                                                                                                                                                                                                                                 DataInputStream d = new DataInputStream(in);
     *                                                                                                                                                                                                                                                 </pre>
     *
     *             </blockquote> with: <blockquote>
     *
     *             <pre>
     *                                                                                                                                                                                                                                                 BufferedReader d = new BufferedReader(new InputStreamReader(in));
     *                                                                                                                                                                                                                                                 </pre>
     *
     *             </blockquote>
     */
    @Deprecated
    public final String readLine() throws IOException {
        char buf[] = lineBuffer;
        if (buf == null) {
            buf = lineBuffer = new char[128];
        }
        int room = buf.length;
        int offset = 0;
        int c;
        loop:
        while (true) {
            switch (c = read()) {
                case -1:
                case '\n':
                    break loop;
                case '\r':
                    final int c2 = read();
                    if ((c2 != '\n') && (c2 != -1)) {
                        new PushbackInputStream(this).unread(c2);
                    }
                    break loop;
                default:
                    if (--room < 0) {
                        buf = new char[offset + 128];
                        room = buf.length - offset - 1;
                        System.arraycopy(lineBuffer, 0, buf, 0, offset);
                        lineBuffer = buf;
                    }
                    buf[offset++] = (char) c;
                    break;
            }
        }
        if ((c == -1) && (offset == 0)) {
            return null;
        }
        return String.copyValueOf(buf, 0, offset);
    }

    /**
     * See the general contract of the <code>readLong</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a
     *         <code>long</code>.
     * @throws EOFException if this input stream reaches the end before reading eight
     *                      bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final long readLong() throws IOException {
        readFully(readBuffer, 0, 8);
        return (((long) readBuffer[0] << 56) + ((long) (readBuffer[1] & 255) << 48)
                + ((long) (readBuffer[2] & 255) << 40) + ((long) (readBuffer[3] & 255) << 32)
                + ((long) (readBuffer[4] & 255) << 24) + ((readBuffer[5] & 255) << 16)
                + ((readBuffer[6] & 255) << 8) + ((readBuffer[7] & 255) << 0));
    }

    /**
     * See the general contract of the <code>readShort</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as a signed
     *         16-bit number.
     * @throws EOFException if this input stream reaches the end before reading two
     *                      bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final short readShort() throws IOException {
        final int ch1 = read();
        final int ch2 = read();
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
     * @throws EOFException if this input stream has reached the end.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final int readUnsignedByte() throws IOException {
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
     * @throws EOFException if this input stream reaches the end before reading two
     *                      bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public final int readUnsignedShort() throws IOException {
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
     * @throws EOFException           if this input stream reaches the end before reading all
     *                                the bytes.
     * @throws IOException            if an I/O error occurs.
     * @throws UTFDataFormatException if the bytes do not represent a valid modified UTF-8
     *                                encoding of a string.
     * @see java.io.DataInputStream#readUTF(java.io.DataInput)
     */
    public final String readUTF() throws IOException {
        boolean isNull = readBoolean();
        if (isNull) return null;
        int length = readInt();
        StringBuilder result = new StringBuilder(length);
        int chunkSize = length / FastByteArrayOutputStream.STRING_CHUNK_SIZE + 1;
        while (chunkSize > 0) {
            result.append(readShortUTF());
            chunkSize--;
        }
        return result.toString();
    }

    private final String readShortUTF() throws IOException {
        final int utflen = readShort();
        byte[] bytearr = null;
        char[] chararr = null;
        bytearr = new byte[utflen];
        chararr = new char[utflen];
        int c, char2, char3;
        int count = 0;
        int chararr_count = 0;
        readFully(bytearr, 0, utflen);
        while (count < utflen) {
            c = bytearr[count] & 0xff;
            if (c > 127)
                break;
            count++;
            chararr[chararr_count++] = (char) c;
        }
        while (count < utflen) {
            c = bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx */
                    count++;
                    chararr[chararr_count++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx 10xx xxxx */
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    char2 = bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    char2 = bytearr[count - 2];
                    char3 = bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException("malformed input around byte " + (count - 1));
                    chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }
}
