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

import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.nio.Bits;

/**
 * Provides methods to read key-value storage from some storage (disk or memory);
 * <p>
 * Storage format is:
 * <p>
 * <pre>
 * ---------------------------------------------------------------------------------------------------
 * |int        |long    |int         |int      |long        |long     |byte[]  |long     |byte[]     |
 * |partitionID|hashCode|segmentCount|segmentID|recordsCount|keySize  |keyBytes|valueSize|valueBytes |
 * ---------------------------------------------------------------------------------------------------
 * </pre>
 */
public class SpillFileCursor {
    private static final int SLOT_SIZE = 3 * Bits.INT_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES;

    private static final int SOURCE_SIZE = Bits.INT_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES;

    private SpillFileReader spillFileReader;

    private int segmentCount;
    private long recordCountInCurrentSegment;
    private long recordAddress;
    private long hashCode;
    private int partitionId;
    private int segmentsRead;
    private long recordsRead;

    /** Assigns a new spill file reader to key-value reader. */
    public void open(SpillFileReader reader) {
        spillFileReader = reader;
    }

    public boolean slotAdvance() {
        if (!hasNextSlot()) {
            return false;
        }
        segmentsRead = 0;
        partitionId = spillFileReader.readNextInt();
        hashCode = spillFileReader.readNextLong();
        segmentCount = spillFileReader.readNextInt();
        return true;
    }

    public boolean segmentAdvance() {
        if (!hasNextSegment()) {
            return false;
        }
        segmentsRead++;
        recordsRead = 0;
        // reads segmentId, but not used anywhere. Inspect whether OK to remove segmentId.
        spillFileReader.readNextInt();
        recordCountInCurrentSegment = spillFileReader.readNextLong();
        return true;
    }

    /**
     * Transfers the next record from the spill file to the given memory block at the given offset.
     *
     * @param memoryBlock destination memory block for the record
     * @param offset destination offset within the memory block
     */
    public boolean recordAdvance(MemoryBlock memoryBlock, long offset, boolean resetMemoryBlock) {
        if (!hasNextRecord()) {
            return false;
        }
        if (resetMemoryBlock) {
            memoryBlock.reset();
        }
        recordAddress = offset;
        offset = readNextPart(memoryBlock, offset);
        readNextPart(memoryBlock, offset);
        recordsRead++;
        return true;
    }

    /** Returns the partition ID of the last fetched key. */
    public int getPartitionId() {
        return partitionId;
    }

    /** Returns the address of the last read record inside the corresponding MemoryBlock. */
    public long getRecordAddress() {
        return recordAddress;
    }

    /** Returns the number of values for the last fetched key. */
    public long getRecordCountInCurrentSegment() {
        return recordCountInCurrentSegment;
    }

    /** Returns the hashcode of the last read key. */
    public long getHashCode() {
        return hashCode;
    }

    /** Closes and finalizes the reader. */
    public void close() {
        segmentsRead = 0;
        recordsRead = 0;
        segmentCount = 0;
        recordCountInCurrentSegment = 0;
    }

    private boolean hasNextSlot() {
        return spillFileReader != null && spillFileReader.ensureAvailable(SLOT_SIZE);
    }

    private boolean hasNextSegment() {
        return spillFileReader != null && spillFileReader.ensureAvailable(SOURCE_SIZE) && segmentCount > segmentsRead;
    }

    private boolean hasNextRecord() {
        return recordCountInCurrentSegment > recordsRead;
    }

    private long readNextPart(MemoryBlock memoryBlock, long offset) {
        long size = spillFileReader.readNextLong();
        memoryBlock.getAccessor().putLong(offset, size);
        offset += Bits.LONG_SIZE_IN_BYTES;
        spillFileReader.readBlob(memoryBlock, offset, size);
        offset += size;
        return offset;
    }
}
