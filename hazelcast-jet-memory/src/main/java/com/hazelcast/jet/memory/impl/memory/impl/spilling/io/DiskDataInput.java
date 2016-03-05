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

package com.hazelcast.jet.memory.impl.memory.impl.spilling.io;

import com.hazelcast.nio.Bits;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.internal.memory.impl.ByteArrayAccessStrategy;
import com.hazelcast.jet.memory.api.memory.spilling.io.SpillingDataInput;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Provides methods to read spilled data from disk;
 */
public class DiskDataInput
        implements SpillingDataInput<InputStream> {
    /**
     * Pointer on the last byte which can be read from the
     * spilling buffer;
     */
    protected int readPointer;

    /**
     * Spilling buffer;
     */
    protected final byte[] spillingBuffer;

    /**
     * Pointer on the last  byte inside spilling buffer
     * which can be used for write from the file;
     */
    private int spillingPointer;

    private boolean useBigEndian;

    private InputStream inputStream;

    public DiskDataInput(int spillingBufferSize,
                         boolean useBigEndian) {
        assert spillingBufferSize >= Bits.LONG_SIZE_IN_BYTES;
        this.useBigEndian = useBigEndian;
        this.spillingBuffer = new byte[spillingBufferSize];
    }

    @Override
    public void open(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public boolean checkAvailable(int size) {
        try {
            return spillingPointer - readPointer >= size || readBuffer(size);
        } catch (IOException e) {
            return false;
        }
    }

    private boolean readBuffer(int size) throws IOException {
        int delta = spillingPointer - readPointer;

        if (delta > 0) {
            System.arraycopy(spillingBuffer, readPointer, spillingBuffer, 0, delta);
        }

        readPointer = 0;
        spillingPointer = delta;

        while (spillingPointer < size) {
            int readBytes = inputStream.read(
                    spillingBuffer,
                    spillingPointer,
                    spillingBuffer.length - spillingPointer
            );

            if (readBytes < 0) {
                if (spillingPointer < size) {
                    throw new EOFException("Not enough data in stream, required=" + size);
                }

                return false;
            } else {
                shiftSpillingPointer(readBytes);
            }
        }

        return true;
    }

    @Override
    public void readBlob(long offset, long blobSize, MemoryBlock memoryBlock) {
        /**
         * BufferPointer:
         *
         * ------------------------------------------------------
         * | 0     readPointer  |       spillingPointer         |
         * |                    |                               |
         * ------------------------------------------------------
         */

        assert memoryBlock != null;

        long remainingBytesToRead = blobSize;

        while (remainingBytesToRead > 0) {
            int availableForRead = spillingPointer - readPointer;

            if (availableForRead <= 0) {
                int size = (int) Math.min(remainingBytesToRead, spillingBuffer.length);

                if (!checkAvailable(size)) {
                    throw new IllegalStateException(
                            "Can't read next " + size + " bytes from spilled file (end of file), " +
                                    "operation: (readBlob, size=" + blobSize + ")"
                    );
                }
            }

            int bytesToRead = (int) Math.min(remainingBytesToRead, availableForRead);

            memoryBlock.copyFromByteArray(
                    spillingBuffer,
                    readPointer,
                    offset,
                    bytesToRead
            );

            readPointer += bytesToRead;
            offset += bytesToRead;
            remainingBytesToRead -= bytesToRead;
        }
    }

    @Override
    public void stepOnBytes(long size) {
        try {
            long skipped = 0;

            while (skipped < size) {
                skipped += inputStream.skip(size - skipped);
            }
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }

    @Override
    public boolean isBigEndian() {
        return useBigEndian;
    }

    @Override
    public int readNextInt() {
        if (!checkAvailable(Bits.INT_SIZE_IN_BYTES)) {
            throw new IllegalStateException(
                    "Can't read next " + Bits.INT_SIZE_IN_BYTES +
                            " bytes from spilled file (end of file), " +
                            "operation: (readNextInt, size=" + Bits.INT_SIZE_IN_BYTES + ")"
            );
        }

        int result = EndiannessUtil.readInt(
                ByteArrayAccessStrategy.INSTANCE,
                spillingBuffer,
                readPointer,
                useBigEndian
        );

        shiftReadPointer(Bits.INT_SIZE_IN_BYTES);
        return result;
    }

    @Override
    public long readNextLong() {
        if (!checkAvailable(Bits.LONG_SIZE_IN_BYTES)) {
            throw new IllegalStateException(
                    "Can't read next " + Bits.LONG_SIZE_IN_BYTES +
                            " bytes from spilled file (end of file), " +
                            "operation: (readNextLong, size=" + Bits.LONG_SIZE_IN_BYTES + ") "
            );
        }

        long result = EndiannessUtil.readLong(
                ByteArrayAccessStrategy.INSTANCE,
                spillingBuffer,
                readPointer,
                useBigEndian
        );

        shiftReadPointer(Bits.LONG_SIZE_IN_BYTES);
        return result;
    }

    @Override
    public void close() {
        closeInputStream();
    }

    private void closeInputStream() {
        try {
            this.inputStream.close();
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }

    private void shiftReadPointer(int readBytes) {
        readPointer += readBytes;
    }

    private void shiftSpillingPointer(int readBytes) {
        spillingPointer += readBytes;
    }
}
