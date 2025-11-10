/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.ArrayUtils;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.version.Version.UNKNOWN;

@SuppressWarnings({"MagicNumber", "MethodCount"})
class ByteArrayObjectDataInput extends VersionedObjectDataInput implements BufferObjectDataInput {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    byte[] data;
    int size;
    int pos;
    int mark;
    char[] charBuffer;

    private final InternalSerializationService service;
    private final boolean bigEndian;
    private final boolean isCompatibility;

    ByteArrayObjectDataInput(byte[] data,
                             InternalSerializationService service,
                             ByteOrder byteOrder) {
        this(data, 0, service, byteOrder, false);
    }

    ByteArrayObjectDataInput(byte[] data,
                             InternalSerializationService service,
                             ByteOrder byteOrder,
                             boolean isCompatibility) {
        this(data, 0, service, byteOrder, isCompatibility);
    }

    ByteArrayObjectDataInput(byte[] data,
                             int offset,
                             InternalSerializationService service,
                             ByteOrder byteOrder,
                             boolean isCompatibility) {
        this.data = data;
        this.size = data != null ? data.length : 0;
        this.pos = offset;
        this.service = service;
        this.bigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
        this.isCompatibility = isCompatibility;
    }

    @Override
    public void init(byte[] data, int offset) {
        this.data = data;
        this.size = data != null ? data.length : 0;
        this.pos = offset;
    }

    @Override
    public void clear() {
        data = null;
        size = 0;
        pos = 0;
        mark = 0;
        if (charBuffer != null && charBuffer.length > UTF_BUFFER_SIZE * 8) {
            charBuffer = new char[UTF_BUFFER_SIZE * 8];
        }
        version = UNKNOWN;
        wanProtocolVersion = UNKNOWN;
    }

    @Override
    public int read() throws EOFException {
        return (pos < size) ? (data[pos++] & 0xff) : -1;
    }

    @Override
    public int read(int position) throws EOFException {
        return (position < size) ? (data[position] & 0xff) : -1;
    }

    @Override
    public final int read(byte[] b, int off, int len) throws EOFException {
        if (b == null) {
            throw new NullPointerException();
        } else {
            ArrayUtils.boundsCheck(b.length, off, len);
        }
        if (len == 0) {
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

    @Override
    public final boolean readBoolean() throws EOFException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    @Override
    public final boolean readBoolean(int position) throws EOFException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    /**
     * See the general contract of the {@code readByte} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream as a signed 8-bit
     * {@code byte}.
     * @throws java.io.EOFException if this input stream has reached the end
     * @see java.io.FilterInputStream#in
     */
    @Override
    public final byte readByte() throws EOFException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    @Override
    public final byte readByte(int position) throws EOFException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    /**
     * See the general contract of the {@code readChar} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream as a Unicode character
     * @throws java.io.EOFException if this input stream reaches the end before reading two bytes
     * @see java.io.FilterInputStream#in
     */
    @Override
    public final char readChar() throws EOFException {
        final char c = readChar(pos);
        pos += CHAR_SIZE_IN_BYTES;
        return c;
    }

    @Override
    public char readChar(int position) throws EOFException {
        checkAvailable(position, CHAR_SIZE_IN_BYTES);
        return Bits.readChar(data, position, bigEndian);
    }

    /**
     * See the general contract of the {@code readDouble} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a {@code double}
     * @throws java.io.EOFException if this input stream reaches the end before reading eight bytes
     * @see java.io.DataInputStream#readLong()
     * @see Double#longBitsToDouble(long)
     */
    @Override
    public double readDouble() throws EOFException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public double readDouble(int position) throws EOFException {
        return Double.longBitsToDouble(readLong(position));
    }

    @Override
    public double readDouble(ByteOrder byteOrder) throws EOFException {
        return Double.longBitsToDouble(readLong(byteOrder));
    }

    @Override
    public double readDouble(int position, ByteOrder byteOrder) throws EOFException {
        return Double.longBitsToDouble(readLong(position, byteOrder));
    }

    /**
     * See the general contract of the {@code readFloat} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as a {@code float}
     * @throws java.io.EOFException if this input stream reaches the end before reading four bytes
     * @see java.io.DataInputStream#readInt()
     * @see Float#intBitsToFloat(int)
     */
    @Override
    public float readFloat() throws EOFException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public float readFloat(int position) throws EOFException {
        return Float.intBitsToFloat(readInt(position));
    }

    @Override
    public float readFloat(ByteOrder byteOrder) throws EOFException {
        return Float.intBitsToFloat(readInt(byteOrder));
    }

    @Override
    public float readFloat(int position, ByteOrder byteOrder) throws EOFException {
        return Float.intBitsToFloat(readInt(position, byteOrder));
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
        if (read(b) == -1) {
            throw new EOFException("End of stream reached");
        }
    }

    @Override
    public void readFully(final byte[] b, final int off, final int len) throws EOFException {
        if (read(b, off, len) == -1) {
            throw new EOFException("End of stream reached");
        }
    }

    /**
     * See the general contract of the {@code readInt} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as an {@code int}
     * @throws java.io.EOFException if this input stream reaches the end before reading four bytes
     * @see java.io.FilterInputStream#in
     */
    @Override
    public final int readInt() throws EOFException {
        final int i = readInt(pos);
        pos += INT_SIZE_IN_BYTES;
        return i;
    }

    @Override
    public int readInt(int position) throws EOFException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return Bits.readInt(data, position, bigEndian);
    }

    @Override
    public final int readInt(ByteOrder byteOrder) throws EOFException {
        final int i = readInt(pos, byteOrder);
        pos += INT_SIZE_IN_BYTES;
        return i;
    }

    @Override
    public int readInt(int position, ByteOrder byteOrder) throws EOFException {
        checkAvailable(position, INT_SIZE_IN_BYTES);
        return Bits.readInt(data, position, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public final String readLine() {
        throw new UnsupportedOperationException();
    }

    /**
     * See the general contract of the {@code readLong} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a {@code long}
     * @throws java.io.EOFException if this input stream reaches the end before reading eight bytes
     * @see java.io.FilterInputStream#in
     */
    @Override
    public final long readLong() throws EOFException {
        final long l = readLong(pos);
        pos += LONG_SIZE_IN_BYTES;
        return l;
    }

    @Override
    public long readLong(int position) throws EOFException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return Bits.readLong(data, position, bigEndian);
    }

    @Override
    public final long readLong(ByteOrder byteOrder) throws EOFException {
        final long l = readLong(pos, byteOrder);
        pos += LONG_SIZE_IN_BYTES;
        return l;
    }

    @Override
    public long readLong(int position, ByteOrder byteOrder) throws EOFException {
        checkAvailable(position, LONG_SIZE_IN_BYTES);
        return Bits.readLong(data, position, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    /**
     * See the general contract of the {@code readShort} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as a signed 16-bit number
     * @throws java.io.EOFException if this input stream reaches the end before reading two bytes
     * @see java.io.FilterInputStream#in
     */
    @Override
    public final short readShort() throws EOFException {
        short s = readShort(pos);
        pos += SHORT_SIZE_IN_BYTES;
        return s;
    }

    @Override
    public short readShort(int position) throws EOFException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        return Bits.readShort(data, position, bigEndian);
    }

    @Override
    public final short readShort(ByteOrder byteOrder) throws EOFException {
        short s = readShort(pos, byteOrder);
        pos += SHORT_SIZE_IN_BYTES;
        return s;
    }

    @Override
    public short readShort(int position, ByteOrder byteOrder) throws EOFException {
        checkAvailable(position, SHORT_SIZE_IN_BYTES);
        return Bits.readShort(data, position, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    @Nullable
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
        return EMPTY_BYTE_ARRAY;
    }

    @Override
    @Nullable
    public boolean[] readBooleanArray() throws EOFException {
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
    @Nullable
    public char[] readCharArray() throws EOFException {
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
    @Nullable
    public int[] readIntArray() throws EOFException {
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
    @Nullable
    public long[] readLongArray() throws EOFException {
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
    @Nullable
    public double[] readDoubleArray() throws EOFException {
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
    @Nullable
    public float[] readFloatArray() throws EOFException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            float[] values = new float[len];
            int sizeInBytes = len * FLOAT_SIZE_IN_BYTES;
            checkAvailable(pos, sizeInBytes);
            ByteBuffer.wrap(data, pos, sizeInBytes).order(getByteOrder()).asFloatBuffer().get(values);
            pos += sizeInBytes;
            return values;
        }
        return new float[0];
    }

    @Override
    @Nullable
    public short[] readShortArray() throws EOFException {
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
    @Nullable
    @Deprecated
    public String[] readUTFArray() throws IOException {
        return readStringArray();
    }

    @Override
    @Nullable
    public String[] readStringArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            String[] values = new String[len];
            for (int i = 0; i < len; i++) {
                values[i] = readString();
            }
            return values;
        }
        return new String[0];
    }

    /**
     * See the general contract of the {@code readUnsignedByte} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream, interpreted as an unsigned 8-bit number
     * @throws java.io.EOFException if this input stream has reached the end
     * @see java.io.FilterInputStream#in
     */
    @Override
    public int readUnsignedByte() throws EOFException {
        return readByte() & 0xFF;
    }

    /**
     * See the general contract of the {@code readUnsignedShort} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as an unsigned 16-bit integer
     * @throws java.io.EOFException if this input stream reaches the end before reading two bytes
     * @see java.io.FilterInputStream#in
     */
    @Override
    public int readUnsignedShort() throws EOFException {
        return readShort() & 0xffff;
    }

    /**
     * See the general contract of the {@code readUTF} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return a Unicode string.
     * @throws java.io.EOFException           if this input stream reaches the end before reading all the bytes
     * @throws java.io.IOException            if an I/O error occurs
     * @throws java.io.UTFDataFormatException if the bytes do not represent a valid modified UTF-8 encoding of a string
     * @see java.io.DataInputStream#readUTF(java.io.DataInput)
     */
    @Override
    @Deprecated
    public final String readUTF() throws IOException {
        return readString();
    }

    @Nullable
    @Override
    public String readString() throws IOException {
        return isCompatibility ? readUTFInternalCompatibility() : readUTFInternal();
    }

    @Override
    @Nullable
    public final Object readObject() throws EOFException {
        return service.readObject(this);
    }

    @Override
    @Nullable
    public <T> T readObject(Class aClass) throws IOException {
        return service.readObject(this, aClass);
    }

    @Override
    @Nullable
    public <T> T readDataAsObject() throws IOException {
        // a future optimization would be to skip the construction of the Data object
        Data data = readData();
        return data == null ? null : (T) service.toObject(data);
    }

    @Override
    @Nullable
    public final Data readData() throws IOException {
        byte[] bytes = readByteArray();
        return bytes == null ? null : new HeapData(bytes);
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

    final void checkAvailable(int pos, int k) throws EOFException {
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
        data = null;
        charBuffer = null;
    }

    @Override
    public final ClassLoader getClassLoader() {
        return service.getClassLoader();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return service;
    }

    @Override
    public ByteOrder getByteOrder() {
        return bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    private String readUTFInternal() throws EOFException {
        int numberOfBytes = readInt();
        if (numberOfBytes == NULL_ARRAY_LENGTH) {
            return null;
        }

        String result = new String(data, pos, numberOfBytes, StandardCharsets.UTF_8);
        pos += numberOfBytes;
        return result;
    }

    public final String readUTFInternalCompatibility() throws IOException {
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
                charBuffer[i] = Bits.readUtf8CharCompatibility(this, b);
            } else {
                charBuffer[i] = (char) b;
            }
        }
        return new String(charBuffer, 0, charCount);
    }

    @Override
    public String toString() {
        return "ByteArrayObjectDataInput{"
                + "size=" + size
                + ", pos=" + pos
                + ", mark=" + mark
                + '}';
    }
}
