/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.exception.LogValidationException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static com.hazelcast.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static java.lang.Math.min;

public class BufferedRaf implements Closeable {
    private static final int BUFFER_SIZE = 1 << 12;

    private enum Mode { READING, WRITING }

    private final RandomAccessFile raf;
    private final byte[] buf = new byte[BUFFER_SIZE];
    private final ByteBuffer auxBuf = ByteBuffer.wrap(new byte[LONG_SIZE_IN_BYTES]);
    private Mode mode;
    private long fileLength;
    private long bufBaseFileOffset;
    private int bufLimit;
    private long filePointer;

    BufferedRaf(RandomAccessFile raf) throws IOException {
        this.raf = raf;
        this.fileLength = raf.length();
    }

    public ObjectDataOutputStream asObjectDataOutputStream(InternalSerializationService serde) {
        return new BufRafObjectDataOut(new BufRafOutputStream(), serde);
    }

    public ObjectDataInputStream asObjectDataInputStream(InternalSerializationService serde) {
        return new BufRafObjectDataIn(new BufRafInputStream(), serde);
    }

    public long length() {
        return fileLength;
    }

    public void setLength(long newLength) throws IOException {
        flush();
        raf.setLength(newLength);
        this.fileLength = newLength;
    }

    public long filePointer() {
        return filePointer;
    }

    public void flush() throws IOException {
        if (mode == Mode.WRITING) {
            flushBuffer();
        }
    }

    public void force() throws IOException {
        flush();
        raf.getFD().sync();
    }

    @Override
    public void close() throws IOException {
        flush();
        raf.close();
    }

    public void seek(long offset) {
        Preconditions.checkNotNegative(offset, "Asked to seek to a negative file offset ");
        filePointer = offset;
    }

    public void skipBytes(long length) {
        seek(filePointer + length);
    }

    public void read(byte[] buffer, int start, int count) throws IOException {
        if (start + count > buffer.length) {
            throw new IllegalArgumentException("Requested to read more than fits into the buffer");
        }
        ensure(Mode.READING);
        if (filePointer + count > fileLength) {
            throw new IllegalArgumentException("Requested to read beyond the end of file");
        }
        fillBufferIfNeeded();
        int bytesRead = 0;
        while (true) {
            int srcOffset = (int) (filePointer - bufBaseFileOffset);
            int bytesToCopy = min(count - bytesRead, bufLimit - srcOffset);
            System.arraycopy(buf, srcOffset, buffer, start + bytesRead, bytesToCopy);
            bytesRead += bytesToCopy;
            filePointer += bytesToCopy;
            if (bytesRead == count) {
                return;
            }
            fillBuffer();
        }
    }

    public void copyTo(DataOutput dest) throws IOException {
        ensure(Mode.READING);
        fillBufferIfNeeded();
        while (filePointer != fileLength) {
            int srcOffset = (int) (filePointer - bufBaseFileOffset);
            int bytesToCopy = min((int) (fileLength - filePointer), bufLimit - srcOffset);
            dest.write(buf, srcOffset, bytesToCopy);
            filePointer += bytesToCopy;
            fillBuffer();
        }
    }

    private void fillBufferIfNeeded() throws IOException {
        long bufOffset = filePointer - bufBaseFileOffset;
        if (bufOffset < 0 || bufOffset > bufLimit) {
            fillBuffer();
        }
    }

    public void write(@Nonnull byte[] bytes, int start, int count) throws IOException {
        ensure(Mode.WRITING);
        if (filePointer != bufBaseFileOffset + bufLimit) {
            // It means that this write request is non-contiguous with the previous.
            flushBuffer();
            bufBaseFileOffset = filePointer;
        }
        if (filePointer + count > fileLength) {
            fileLength = filePointer + count;
        }
        int doneCount = 0;
        do {
            int copySize = min(count - doneCount, buf.length - bufLimit);
            System.arraycopy(bytes, start + doneCount, buf, bufLimit, copySize);
            bufLimit += copySize;
            if (bufLimit == buf.length) {
                flushBuffer();
            }
            doneCount += copySize;
        } while (doneCount != count);
        filePointer += count;
    }

    public int readByte() throws IOException {
        read(auxBuf.array(), 0, BYTE_SIZE_IN_BYTES);
        return auxBuf.get(0);
    }

    public int readInt() throws IOException {
        read(auxBuf.array(), 0, INT_SIZE_IN_BYTES);
        return auxBuf.getInt(0);
    }

    public long readLong() throws IOException {
        read(auxBuf.array(), 0, LONG_SIZE_IN_BYTES);
        return auxBuf.getLong(0);
    }

    public void writeByte(int b) throws IOException {
        auxBuf.put(0, (byte) b);
        write(auxBuf.array(), 0, BYTE_SIZE_IN_BYTES);
    }

    public void writeInt(int value) throws IOException {
        auxBuf.putInt(0, value);
        write(auxBuf.array(), 0, INT_SIZE_IN_BYTES);
    }

    public void writeLong(long value) throws IOException {
        auxBuf.putLong(0, value);
        write(auxBuf.array(), 0, LONG_SIZE_IN_BYTES);
    }

    private void ensure(Mode requestedMode) throws IOException {
        if (mode == requestedMode) {
            return;
        }
        if (mode == Mode.WRITING) {
            flushBuffer();
        } else if (mode == Mode.READING) {
            bufLimit = 0;
        }
        mode = requestedMode;
    }

    private void flushBuffer() throws IOException {
        raf.seek(bufBaseFileOffset);
        raf.write(buf, 0, bufLimit);
        bufBaseFileOffset += bufLimit;
        bufLimit = 0;
    }

    private void fillBuffer() throws IOException {
        raf.seek(filePointer);
        bufLimit = min(buf.length, (int) (fileLength - filePointer));
        int offset = 0;
        do {
            int readCount = raf.read(buf, offset, bufLimit - offset);
            if (readCount == -1) {
                throw new IOException("Unexpected EOF reached at " + raf.getFilePointer());
            }
            offset += readCount;
        } while (offset != bufLimit);
        bufBaseFileOffset = filePointer;
    }

    private class BufRafInputStream extends InputStream {
        private final CRC32 crc32 = new CRC32();

        @Override
        public int read() throws IOException {
            int b = BufferedRaf.this.readByte();
            crc32.update(b);
            return b;
        }

        @Override
        public int read(@Nonnull byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(@Nonnull byte[] b, int off, int len) throws IOException {
            int readCount = (int) Math.min(len, fileLength - filePointer);
            BufferedRaf.this.read(b, off, len);
            crc32.update(b, off, len);
            return readCount;
        }

        void checkCrc32() throws IOException {
            if ((int) crc32.getValue() != BufferedRaf.this.readInt()) {
                throw new LogValidationException("CRC failure");
            }
            crc32.reset();
        }
    }

    private class BufRafOutputStream extends OutputStream {
        private final CRC32 crc32 = new CRC32();

        @Override
        public void write(int b) throws IOException {
            BufferedRaf.this.writeByte(b);
            crc32.update(b);
        }

        @Override
        public void write(@Nonnull byte[] b) throws IOException {
            BufferedRaf.this.write(b, 0, b.length);
            crc32.update(b);
        }

        @Override
        public void write(@Nonnull byte[] b, int off, int len) throws IOException {
            BufferedRaf.this.write(b, off, len);
            crc32.update(b, off, len);
        }

        void writeCrc32() throws IOException {
            BufferedRaf.this.writeInt((int) crc32.getValue());
            crc32.reset();
        }
    }

    private class BufRafObjectDataIn extends ObjectDataInputStream {

        private final BufRafInputStream inputStream;

        BufRafObjectDataIn(BufRafInputStream inputStream, InternalSerializationService serializationService) {
            super(inputStream, serializationService);
            this.inputStream = inputStream;
        }

        @Override
        public <T> T readObject() throws IOException {
            T object = super.readObject();
            inputStream.checkCrc32();
            return object;
        }
    }

    private class BufRafObjectDataOut extends ObjectDataOutputStream {

        private final BufRafOutputStream outputStream;

        BufRafObjectDataOut(BufRafOutputStream outputStream, InternalSerializationService serializationService) {
            super(outputStream, serializationService);
            this.outputStream = outputStream;
        }

        @Override
        public void writeObject(Object object) throws IOException {
            super.writeObject(object);
            outputStream.writeCrc32();
        }
    }
}
