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

package com.hazelcast.jet.memory.spilling;

import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.jet.memory.JetMemoryException;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.util.Util;
import com.hazelcast.nio.Bits;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.BYTE_ARRAY_ACCESS;

/**
 * Reader of a spill-file.
 */
public final class SpillFileReader implements Closeable {

    private final boolean isBigEndian;

    private final byte[] buffer;

    private InputStream input;

    /** Position inside the buffer of the next byte to be read. */
    private int readPosition;

    /**
     * Position inside the buffer of the next free byte, where more data can be copied from the file,
     * and up to which data can be read.
     */
    private int readLimit;

    public SpillFileReader(int bufSize, boolean isBigEndian) {
        assert bufSize >= Bits.LONG_SIZE_IN_BYTES;
        this.isBigEndian = isBigEndian;
        this.buffer = new byte[bufSize];
    }


    /** Makes this reader use the supplied file input stream as the source. It is illegal to call
     * this method while there is still unread data in the buffer. */
    public void setInput(FileInputStream in) {
        assert readyInBuffer() == 0 : "Attempt to set input stream while buffer not empty";
        this.input = in;
    }

    /**
     * Tries to ensure that at least {@code count} bytes are available to be read directly from
     * the internal buffer, by prefetching them from the underlying data source.
     * It is illegal to request more bytes than fit into the internal buffer.
     *
     * @param count number of bytes to ensure
     * @return {@code true} if the requested number of bytes is now available, {@code false}
     * if less than that is available because end of stream was reached
     * @throws JetMemoryException if the underlying I/O operation fails for a reason other than
     * end of stream
     */
    @SuppressWarnings("checkstyle:innerassignment")
    public boolean ensureAvailable(int count) {
        assert count <= buffer.length : String.format(
                "Requested to make %d bytes available, but buffer size is %d", count, buffer.length);
        if (readyInBuffer() >= count) {
            return true;
        }
        compact();
        for (int freeInBuffer; (freeInBuffer = freeInBuffer()) > 0;) {
            try {
                final int readCount = input.read(buffer, readLimit, freeInBuffer);
                if (readCount < 0) {
                    return readyInBuffer() >= count;
                }
                readLimit += readCount;
            } catch (IOException e) {
                throw new JetMemoryException("Reading from spill file failed", e);
            }
        }
        return true;
    }

    /** Reads and returns the next {@code int} value from the underlying data source. */
    public int readNextInt() {
        if (!ensureAvailable(Bits.INT_SIZE_IN_BYTES)) {
            throw new JetMemoryException("Reached end of file while trying to read an int value from spilled file");
        }
        int result = EndiannessUtil.readInt(BYTE_ARRAY_ACCESS, buffer, readPosition, isBigEndian);
        readPosition += Bits.INT_SIZE_IN_BYTES;
        return result;
    }

    /** Reads and returns the next {@code long} value from the underlying data source. */
    public long readNextLong() {
        if (!ensureAvailable(Bits.LONG_SIZE_IN_BYTES)) {
            throw new JetMemoryException("Reached end of file while trying to read a long value from spilled file");
        }
        long result = EndiannessUtil.readLong(BYTE_ARRAY_ACCESS, buffer, readPosition, isBigEndian);
        readPosition += Bits.LONG_SIZE_IN_BYTES;
        return result;
    }

    /**
     * Transfers {@code length} bytes into the supplied {@code MemoryBlock}. The first byte
     * will be stored at the supplied {@code offset} within the memory block.
     */
    public void readBlob(MemoryBlock memoryBlock, long offset, long length) {
        assert memoryBlock != null : "readBlob() called with null memory block";
        long writePosition = offset;
        for (long remainingToTransfer = length; remainingToTransfer > 0;) {
            if (!ensureAvailable((int) Math.min(remainingToTransfer, buffer.length))) {
                throw new JetMemoryException(String.format(
                        "Request to read %d bytes failed due to end of stream", length));
            }
            final int transferredCount = (int) Math.min(remainingToTransfer, readyInBuffer());
            memoryBlock.getAccessor().copyFromByteArray(buffer, readPosition, writePosition, transferredCount);
            readPosition += transferredCount;
            writePosition += transferredCount;
            remainingToTransfer -= transferredCount;
        }
    }

    /** Closes this data input. */
    public void close() {
        try {
            input.close();
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }

    private int readyInBuffer() {
        return readLimit - readPosition;
    }

    private int freeInBuffer() {
        return buffer.length - readLimit;
    }

    private void compact() {
        final int remaining = readyInBuffer();
        if (remaining > 0) {
            System.arraycopy(buffer, readPosition, buffer, 0, remaining);
        }
        readPosition = 0;
        readLimit = remaining;
    }
}
