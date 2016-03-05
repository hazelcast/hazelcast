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

package com.hazelcast.jet.memory.impl.memory.impl.spilling.format;

import com.hazelcast.nio.Bits;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.spilling.io.SpillingDataInput;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueReader;

/**
 * Provides methods to read key-value storage from some storage (disk or memory)
 * <p>
 * Storage format is:
 * <p>
 * <pre>
 * -------------------------------------------------------------------------------------------------
 * |int        |long    |int        |int     |long        |long     |byte[]  |long     |byte[]     |
 * |partitionID|hashCode|sourceCount|sourceID|recordsCount|keySize  |keyBytes|valueSize|valueBytes |
 * -------------------------------------------------------------------------------------------------
 * </pre>
 */
public class DefaultSpillingKeyValueReader
        implements SpillingKeyValueReader {
    private static final int SLOT_SIZE = 3 * Bits.INT_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES;

    private static final int SOURCE_SIZE = Bits.INT_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES;

    protected int sourceId;

    protected long position;

    protected long recordAddress;

    protected long hashCode;

    protected int partitionId;

    protected SpillingDataInput spillingDataInput;

    private int sourceRead;

    private long recordsRead;

    protected int sourceCount;

    protected long recordsCount;

    @Override
    public void readNextSlot() {
        sourceRead = 0;
        partitionId = spillingDataInput.readNextInt();
        hashCode = spillingDataInput.readNextLong();
        sourceCount = spillingDataInput.readNextInt();
    }

    @Override
    public boolean hasNextSlot() {
        return spillingDataInput != null
                &&
                spillingDataInput.checkAvailable(SLOT_SIZE);
    }

    @Override
    public void readNextSource() {
        sourceRead++;
        recordsRead = 0;
        sourceId = spillingDataInput.readNextInt();
        recordsCount = spillingDataInput.readNextLong();
    }

    @Override
    public boolean hasNextSource() {
        return spillingDataInput != null
                &&
                spillingDataInput.checkAvailable(SOURCE_SIZE)
                &&
                sourceCount > sourceRead;
    }

    @Override
    public boolean hasNextRecord() {
        return recordsCount > recordsRead;
    }

    @Override
    public void open(SpillingDataInput source) {
        spillingDataInput = source;
    }

    @Override
    public void readNextRecord(long offset, MemoryBlock memoryBlock) {
        if (!hasNextRecord()) {
            throw new IllegalStateException("Need to read next source before");
        }

        recordAddress = offset;
        offset = readNextParty(memoryBlock, offset);
        readNextParty(memoryBlock, offset);
        recordsRead++;
    }

    private long readNextParty(MemoryBlock memoryBlock, long offset) {
        long size = spillingDataInput.readNextLong();
        memoryBlock.putLong(offset, size);
        offset += Bits.LONG_SIZE_IN_BYTES;
        spillingDataInput.readBlob(offset, size, memoryBlock);
        offset += size;
        return offset;
    }

    @Override
    public int getSourceId() {
        return sourceId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public long getRecordAddress() {
        return recordAddress;
    }

    @Override
    public long getRecordsCount() {
        return recordsCount;
    }

    @Override
    public long getHashCode() {
        return hashCode;
    }

    @Override
    public void close() {
        sourceRead = 0;
        recordsRead = 0;
        sourceCount = 0;
        recordsCount = 0;
        position = MemoryUtil.NULL_VALUE;
    }
}
