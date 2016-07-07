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
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.util.Util;
import com.hazelcast.nio.Bits;

import java.io.FileOutputStream;
import java.io.IOException;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.BYTE_ARRAY_ACCESS;

/**
 * Writer to a spill-file.
 */
public final class SpillFileWriter {

    private final boolean useBigEndian;
    private final byte[] buffer;
    private FileOutputStream out;
    private int position;

    public SpillFileWriter(int bufferSize, boolean useBigEndian) {
        assert bufferSize >= Bits.LONG_SIZE_IN_BYTES;
        this.useBigEndian = useBigEndian;
        this.buffer = new byte[bufferSize];
    }

    /**
     * Makes this writer use the supplied file output stream as the destination. It is illegal
     * to call this method while there is still data in the output buffer.
     */
    public void setOutput(FileOutputStream out) {
        assert position == 0 : "Attempt to reset to a new output stream while data still in buffer";
        this.out = out;
    }

    /** Writes a long value to the spill-file. */
    public void writeLong(long value) {
        ensureRoomInBuffer(Bits.LONG_SIZE_IN_BYTES);
        EndiannessUtil.writeLong(BYTE_ARRAY_ACCESS, buffer, position, value, useBigEndian);
        position += Bits.LONG_SIZE_IN_BYTES;
    }

    /** Writes an int value to the spill-file. */
    public void writeInt(int value) {
        ensureRoomInBuffer(Bits.INT_SIZE_IN_BYTES);
        EndiannessUtil.writeInt(BYTE_ARRAY_ACCESS, buffer, position, value, useBigEndian);
        position += Bits.INT_SIZE_IN_BYTES;
    }

    /**
     * Transfers {@code length} bytes from the supplied {@code MemoryBlock} to the spill-file.
     * The first byte will be read from the supplied {@code offset} within the memory block.
     */
    public void writeBlob(MemoryBlock memoryBlock, long offset, long length) {
        assert memoryBlock != null : "writeBlob() called with null memory block";
        long readOffset = offset;
        for (long remainingToTransfer = length; remainingToTransfer > 0;) {
            final int transferredCount = (int) Math.min(remainingToTransfer, buffer.length);
            ensureRoomInBuffer(transferredCount);
            memoryBlock.getAccessor().copyToByteArray(readOffset, buffer, position, transferredCount);
            readOffset += transferredCount;
            position += transferredCount;
            remainingToTransfer -= transferredCount;
        }
    }

    /** Flushes the buffered data to the underlying output stream. */
    public void flush() throws IOException {
        out.write(buffer, 0, position);
        position = 0;
        out.flush();
    }

    /** Closes the underlying output stream. */
    public void close() {
        position = 0;
        if (out != null) {
            closeOutput();
        }
    }

    private void closeOutput() {
        try {
            out.close();
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }

    private void ensureRoomInBuffer(long requiredByteCount) {
        if (position + requiredByteCount <= buffer.length) {
            return;
        }
        try {
            flush();
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }
}
