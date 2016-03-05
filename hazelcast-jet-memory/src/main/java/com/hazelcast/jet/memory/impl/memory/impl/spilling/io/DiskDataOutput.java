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

import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.internal.memory.impl.ByteArrayAccessStrategy;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.spilling.io.SpillingDataOutput;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.nio.Bits;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Provides methods to write spilled data to disk;
 */
public class DiskDataOutput
        implements SpillingDataOutput<OutputStream> {
    private OutputStream ous;
    private int spillingOffset;
    private final boolean useBigEndian;
    private final byte[] spillingBuffer;

    public DiskDataOutput(
            int spillingBufferSize,
            boolean useBigEndian
    ) {
        this.useBigEndian = useBigEndian;
        assert spillingBufferSize >= Bits.LONG_SIZE_IN_BYTES;
        this.spillingBuffer = new byte[spillingBufferSize];
    }

    @Override
    public void open(OutputStream ous) {
        this.ous = ous;
        spillingOffset = 0;
    }

    protected void checkAvailable(long size) {
        if (spillingOffset + size > spillingBuffer.length) {
            try {
                flush();
            } catch (IOException e) {
                throw Util.reThrow(e);
            }
        }
    }

    @Override
    public void writeBlob(MemoryBlock memoryBlock,
                          long dataAddress,
                          long dataSize) {
        long remainingSize = dataSize;

        while (remainingSize > 0L) {
            int length = (int) Math.min(remainingSize, spillingBuffer.length);

            checkAvailable(length);

            memoryBlock.copyToByteArray(
                    dataAddress,
                    spillingBuffer,
                    spillingOffset,
                    length
            );

            remainingSize -= length;
            spillingOffset += length;
        }
    }

    public void writeLong(long payLoad) {
        checkAvailable(Bits.LONG_SIZE_IN_BYTES);

        EndiannessUtil.writeLong(
                ByteArrayAccessStrategy.INSTANCE,
                spillingBuffer,
                spillingOffset,
                payLoad,
                useBigEndian
        );

        spillingOffset += Bits.LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(int payLoad) {
        checkAvailable(Bits.INT_SIZE_IN_BYTES);

        EndiannessUtil.writeInt(
                ByteArrayAccessStrategy.INSTANCE,
                spillingBuffer,
                spillingOffset,
                payLoad,
                useBigEndian
        );

        spillingOffset += Bits.INT_SIZE_IN_BYTES;
    }

    @Override
    public void flush() throws IOException {
        ous.write(spillingBuffer, 0, spillingOffset);
        spillingOffset = 0;
        ous.flush();
    }

    @Override
    public void close() {
        spillingOffset = 0;

        if (ous != null) {
            closeOutput();
        }
    }

    private void closeOutput() {
        try {
            ous.close();
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }
}
