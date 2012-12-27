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

package com.hazelcast.nio;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public final class FastDataOutputStream extends OutputStream implements DataOutput {

    static final int STRING_CHUNK_SIZE = 16 * 1024;

    private byte buf[];

    private int count;

    private final byte writeBuffer[] = new byte[8];

    public FastDataOutputStream() {
        this(32);
    }

    public FastDataOutputStream(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                    + size);
        }
        buf = new byte[size];
    }

    @Override
    public void write(int b) {
        int newcount = count + 1;
        if (newcount > buf.length) {
            byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
            System.arraycopy(buf, 0, newbuf, 0, count);
            buf = newbuf;
        }
        buf[count] = (byte) b;
        count = newcount;
    }

    @Override
    public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        int newcount = count + len;
        if (newcount > buf.length) {
            byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
            System.arraycopy(buf, 0, newbuf, 0, count);
            buf = newbuf;
        }
        System.arraycopy(b, off, buf, count, len);
        count = newcount;
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(buf, 0, count);
    }

    public void set(byte[] buffer) {
        this.buf = buffer;
    }

    public void reset() {
        count = 0;
    }

    public int size() {
        return count;
    }

    public byte toByteArray()[] {
        byte newbuf[] = new byte[count];
        System.arraycopy(buf, 0, newbuf, 0, count);
        return newbuf;
    }

    public byte[] getBytes() {
        return buf;
    }

    public final void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    public final void writeByte(final int v) throws IOException {
        write(v);
    }

    public final void writeBytes(final String s) throws IOException {
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            write((byte) s.charAt(i));
        }
    }

    public final void writeChar(final int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v) & 0xFF);
    }

    public final void writeChars(final String s) throws IOException {
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            final int v = s.charAt(i);
            write((v >>> 8) & 0xFF);
            write((v) & 0xFF);
        }
    }

    public final void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public final void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public final void writeInt(final int v) throws IOException {
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>> 8) & 0xFF);
        write((v) & 0xFF);
    }

    public final void writeLong(final long v) throws IOException {
        writeBuffer[0] = (byte) (v >>> 56);
        writeBuffer[1] = (byte) (v >>> 48);
        writeBuffer[2] = (byte) (v >>> 40);
        writeBuffer[3] = (byte) (v >>> 32);
        writeBuffer[4] = (byte) (v >>> 24);
        writeBuffer[5] = (byte) (v >>> 16);
        writeBuffer[6] = (byte) (v >>> 8);
        writeBuffer[7] = (byte) (v);
        write(writeBuffer, 0, 8);
    }

    public final void writeShort(final int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v) & 0xFF);
    }

    public final void writeUTF(final String str) throws IOException {
        boolean isNull = (str == null);
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

    private final void writeShortUTF(final String str) throws IOException {
        final int strlen = str.length();
        int utflen = 0;
        int c, count = 0;
        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }
//        if (utflen > 65535)
//            throw new UTFDataFormatException("encoded string too long: " + utflen + " bytes");
        final byte[] bytearr = new byte[utflen + 2];
        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) ((utflen) & 0xFF);
        int i;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F)))
                break;
            bytearr[count++] = (byte) c;
        }
        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[count++] = (byte) c;
            } else if (c > 0x07FF) {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | ((c) & 0x3F));
            } else {
                bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | ((c) & 0x3F));
            }
        }
        write(bytearr, 0, utflen + 2);
    }
}
