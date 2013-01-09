/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.BufferOverflowException;

/**
* @mdogan 12/26/12
*/
class ContextAwareDataOutput extends OutputStream implements IndexedObjectDataOutput, SerializationContextAware {

    private static final int DEFAULT_SIZE = 1024 * 4;

    static final int STRING_CHUNK_SIZE = 16 * 1024;

    private final byte longBuffer[] = new byte[8];

    private byte buffer[];

    private final int offset;

    private int pos = 0;

    private final SerializationService service;

    ContextAwareDataOutput(SerializationService service) {
        this(DEFAULT_SIZE, service);
    }

    ContextAwareDataOutput(int size, SerializationService service) {
        this(new byte[size], 0, service);
    }

    private ContextAwareDataOutput(byte[] buffer, int offset, SerializationService service) {
        this.buffer = buffer;
        this.offset = offset;
        this.service = service;
    }

    @Override
    public void write(int b) {
        ensureAvailable(1);
        writeUnsafe(b);
    }

    public void write(int index, int b) {
        buffer[offset + index] = (byte) b;
    }

    @Override
    public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        ensureAvailable(len);
        System.arraycopy(b, off, buffer, offset + pos, len);
        pos += len;
    }

    public void write(int index, byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        System.arraycopy(b, off, buffer, offset + index, len);
    }

    public void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    public void writeBoolean(int index, final boolean v) throws IOException {
        write(index, v ? 1 : 0);
    }

    public void writeByte(final int v) throws IOException {
        write(v);
    }

    public void writeByte(int index, final int v) throws IOException {
        write(index, v);
    }

    public void writeBytes(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len);
        for (int i = 0; i < len; i++) {
            writeUnsafe((byte) s.charAt(i));
        }
    }

    public void writeChar(final int v) throws IOException {
        ensureAvailable(2);
        writeUnsafe((v >>> 8) & 0xFF);
        writeUnsafe((v) & 0xFF);
    }

    public void writeChar(int index, final int v) throws IOException {
        write(index, (v >>> 8) & 0xFF);
        write(index, (v) & 0xFF);
    }

    public void writeChars(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len * 2);
        for (int i = 0; i < len; i++) {
            final int v = s.charAt(i);
            writeUnsafe((v >>> 8) & 0xFF);
            writeUnsafe((v) & 0xFF);
        }
    }

    public void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeDouble(int index, final double v) throws IOException {
        writeLong(index, Double.doubleToLongBits(v));
    }

    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeFloat(int index, final float v) throws IOException {
        writeInt(index, Float.floatToIntBits(v));
    }

    public void writeInt(final int v) throws IOException {
        ensureAvailable(4);
        writeUnsafe((v >>> 24) & 0xFF);
        writeUnsafe((v >>> 16) & 0xFF);
        writeUnsafe((v >>> 8) & 0xFF);
        writeUnsafe((v) & 0xFF);
    }

    public void writeInt(int index, int v) throws IOException {
        write(index, (v >>> 24) & 0xFF);
        write(index + 1, (v >>> 16) & 0xFF);
        write(index + 2, (v >>> 8) & 0xFF);
        write(index + 3, (v) & 0xFF);
    }

    public void writeLong(final long v) throws IOException {
        fillLongBuffer(v);
        write(longBuffer, 0, 8);
    }

    public void writeLong(int index, final long v) throws IOException {
        fillLongBuffer(v);
        write(index, longBuffer, 0, 8);
    }

    private void fillLongBuffer(long v) {
        longBuffer[0] = (byte) (v >>> 56);
        longBuffer[1] = (byte) (v >>> 48);
        longBuffer[2] = (byte) (v >>> 40);
        longBuffer[3] = (byte) (v >>> 32);
        longBuffer[4] = (byte) (v >>> 24);
        longBuffer[5] = (byte) (v >>> 16);
        longBuffer[6] = (byte) (v >>> 8);
        longBuffer[7] = (byte) (v);
    }

    public void writeShort(final int v) throws IOException {
        ensureAvailable(2);
        writeUnsafe((v >>> 8) & 0xFF);
        writeUnsafe((v) & 0xFF);
    }

    public void writeShort(int index, final int v) throws IOException {
        write(index, (v >>> 8) & 0xFF);
        write(index, (v) & 0xFF);
    }

    public void writeUTF(final String str) throws IOException {
        boolean isNull = str == null;
        writeBoolean(isNull);
        if (isNull) return;

        int length = str.length();
        writeInt(length);
        int chunkSize = length / STRING_CHUNK_SIZE + 1;
        for (int i = 0; i < chunkSize; i++) {
            int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
            int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
            writeShortUTF(str.substring(beginIndex, endIndex));
        }
    }

    private void writeUnsafe(int b) {
        buffer[offset + pos++] = (byte) b;
    }

    private void ensureAvailable(int len) {
        if (available() < len) {
            if (offset > 0) {
                throw new BufferOverflowException();
            }
            if (buffer != null) {
                int newCap = Math.max(buffer.length << 1, buffer.length + len);
                byte newBuffer[] = new byte[newCap];
                System.arraycopy(buffer, 0, newBuffer, 0, pos);
                buffer = newBuffer;
            } else {
                buffer = new byte[len > DEFAULT_SIZE / 2 ? len * 2 : DEFAULT_SIZE];
            }
        }
    }

    public void writeObject(Object object) throws IOException {
        service.writeObject(this, object);
    }

    /**
     * Returns this buffer's position.
     */
    public final int position() {
        return pos;
    }

    public void position(int newPos) {
        if ((offset + newPos > buffer.length) || (newPos < 0))
            throw new IllegalArgumentException();
        pos = newPos;
    }

    public int available() {
        return buffer != null ? buffer.length - pos - offset : 0;
    }

    public int size() {
        return pos;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public byte toByteArray()[] {
        if (buffer == null) {
            return new byte[0];
        }
        final byte newBuffer[] = new byte[size()];
        System.arraycopy(buffer, offset, newBuffer, 0, size());
        return newBuffer;
    }

    public ContextAwareDataOutput duplicate() {
        return new ContextAwareDataOutput(buffer, 0, service);
    }

    public ContextAwareDataOutput slice() {
        return new ContextAwareDataOutput(buffer, pos, service);
    }

    public void reset() {
        pos = 0;
    }

    @Override
    public void close() {
        reset();
        buffer = null;
    }

    private void writeShortUTF(final String str) throws IOException {
        final int stringLen = str.length();
        int utfLength = 0;
        int c, count = 0;
            /* use charAt instead of copying String to char array */
        for (int i = 0; i < stringLen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utfLength++;
            } else if (c > 0x07FF) {
                utfLength += 3;
            } else {
                utfLength += 2;
            }
        }
        if (utfLength > 65535) {
            throw new UTFDataFormatException("encoded string too long:"
                    + utfLength + " bytes");
        }
        final byte[] byteArray = new byte[utfLength + 2];
        byteArray[count++] = (byte) ((utfLength >>> 8) & 0xFF);
        byteArray[count++] = (byte) ((utfLength) & 0xFF);
        int i;
        for (i = 0; i < stringLen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F)))
                break;
            byteArray[count++] = (byte) c;
        }
        for (; i < stringLen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                byteArray[count++] = (byte) c;
            } else if (c > 0x07FF) {
                byteArray[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                byteArray[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                byteArray[count++] = (byte) (0x80 | ((c) & 0x3F));
            } else {
                byteArray[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                byteArray[count++] = (byte) (0x80 | ((c) & 0x3F));
            }
        }
        write(byteArray, 0, utfLength + 2);
    }

    public SerializationContext getSerializationContext() {
        return service.getSerializationContext();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ContextAwareDataOutput");
        sb.append("{size=").append(buffer != null ? buffer.length : "NULL");
        sb.append(", offset=").append(offset);
        sb.append(", pos=").append(pos);
        sb.append('}');
        return sb.toString();
    }
}
