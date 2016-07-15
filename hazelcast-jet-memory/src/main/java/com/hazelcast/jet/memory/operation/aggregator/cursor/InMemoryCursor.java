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

package com.hazelcast.jet.memory.operation.aggregator.cursor;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.binarystorage.Storage;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;
import com.hazelcast.jet.memory.binarystorage.cursor.TupleAddressCursor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.multimap.TupleMultimapHsa;
import com.hazelcast.jet.memory.util.JetIoUtil;
import com.hazelcast.jet.memory.util.Util;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Cursor over in-memory tuples.
 */
public class InMemoryCursor extends TupleCursorBase {
    private final Storage serviceKeyValueStorage;
    private long hashCode;
    private int nextPartitionId;
    private int memoryBlockIdx;
    private int nextMemoryBlockIdx;
    private long recordAddress;
    private MemoryBlock memoryBlock;
    private MemoryBlock nextMemoryBlock;
    private Partition partition;
    private MemoryBlockChain memoryBlockChain;
    private Storage storage;
    private SlotAddressCursor slotCursor;
    private TupleAddressCursor tupleCursor;

    public InMemoryCursor(
            Storage serviceKeyValueStorage, MemoryBlock serviceMemoryBlock, MemoryBlock temporaryMemoryBlock,
            Accumulator accumulator, Tuple2 destTuple, Partition[] partitions, StorageHeader header,
            IOContext ioContext, boolean useBigEndian
    ) {
        super(serviceMemoryBlock, temporaryMemoryBlock, accumulator, destTuple, partitions, header, ioContext,
                useBigEndian);
        this.serviceKeyValueStorage = serviceKeyValueStorage;
    }

    @Override
    public boolean advance() {
        if (!advanceTupleCursor()) {
            if (!(checkNextMemoryBlock() || advanceSlotCursor() || checkMemoryBlock() || advancePartition())) {
                return false;
            }
            advanceTupleCursor();
        }
        if (accumulator == null) {
            tupleFetcher.fetch(currentMemoryBlock(), tupleCursor.tupleAddress());
            return true;
        }
        long tupleAddress = tupleCursor.tupleAddress();
        long valueAddress = JetIoUtil.addrOfValueBlockAt(tupleAddress, memoryBlock.getAccessor());
        long valueSize = JetIoUtil.sizeOfValueBlockAt(tupleAddress, memoryBlock.getAccessor());
        consumeTupleCursor(tupleCursor, valueAddress, valueSize, memoryBlock);
        tupleCursor = null;
        consumeOtherBlocks(tupleAddress, valueAddress, valueSize);
        tupleFetcher.fetch(memoryBlock, tupleAddress);
        return true;
    }

    @Override
    public void reset(Comparator comparator) {
        nextPartitionId = 0;
        partition = null;
        memoryBlock = null;
        memoryBlockIdx = 0;
        nextMemoryBlock = null;
        nextMemoryBlockIdx = 0;
        slotCursor = null;
        tupleCursor = null;
        recordAddress = NULL_ADDRESS;
        super.reset(comparator);
    }

    private boolean advanceTupleCursor() {
        if (tupleCursor != null && tupleCursor.advance()) {
            return true;
        }
        tupleCursor = null;
        return false;
    }

    private boolean advanceSlotCursor() {
        if (slotCursor != null && advanceToUnmarkedSlot()) {
            return true;
        }
        slotCursor = null;
        return false;
    }

    private boolean checkNextMemoryBlock() {
        if (accumulator != null || !hasNextBlock()) {
            return false;
        }
        for (int idx = nextMemoryBlockIdx; idx < memoryBlockChain.size(); idx++) {
            nextMemoryBlock = memoryBlockChain.get(idx);
            header.setMemoryBlock(memoryBlock);
            if (header.baseAddress() == NULL_ADDRESS) {
                continue;
            }
            initMemoryBlock(nextMemoryBlock);
            long slotAddress = storage.addrOfSlotWithSameKey(recordAddress, comparator, memoryBlock.getAccessor());
            if (slotAddress != NULL_ADDRESS) {
                long tupleAddress = nextMemoryBlock.getAccessor().getLong(slotAddress);
                long tupleSize = JetIoUtil.sizeOfTupleAt(tupleAddress, nextMemoryBlock.getAccessor());
                JetIoUtil.putByte(tupleAddress, tupleSize + TupleMultimapHsa.MARKER_OFFSET, Util.BYTE_1,
                        nextMemoryBlock.getAccessor()
                );
                tupleCursor = storage.tupleCursor(slotAddress);
                nextMemoryBlockIdx = idx + 1;
                return true;
            }
        }
        header.setMemoryBlock(memoryBlock);
        initMemoryBlock(memoryBlock);
        nextMemoryBlockIdx = memoryBlockChain.size();
        nextMemoryBlock = null;
        return false;
    }

    private boolean hasNextBlock() {
        return memoryBlockChain != null && recordAddress != NULL_ADDRESS && nextMemoryBlockIdx != 0;
    }

    private void initMemoryBlock(MemoryBlock memoryBlock) {
        storage.setMemoryBlock(memoryBlock);
        storage.gotoAddress(header.baseAddress());
    }

    private boolean advanceToUnmarkedSlot() {
        while (slotCursor.advance()) {
            long slotAddress = slotCursor.slotAddress();
            recordAddress = storage.addrOfFirstTuple(slotAddress);
            if (storage.getSlotMarker(slotAddress) == Util.BYTE_1) {
                continue;
            }
            hashCode = storage.getSlotHashCode(slotAddress);
            tupleCursor = storage.tupleCursor(slotAddress);
            storage.markSlot(slotAddress, Util.BYTE_1);
            nextMemoryBlockIdx = memoryBlockIdx;
            return true;
        }
        return false;
    }

    protected boolean checkMemoryBlock() {
        if (partition == null) {
            return false;
        }
        while (memoryBlockIdx <= partition.getMemoryBlockChain().size() - 1) {
            memoryBlock = memoryBlockChain.get(memoryBlockIdx);
            storage = partitions[partition.getPartitionId()].getStorage();
            header.setMemoryBlock(memoryBlock);
            if (header.baseAddress() == NULL_ADDRESS) {
                memoryBlockIdx++;
                continue;
            }
            initMemoryBlock(memoryBlock);
            slotCursor = storage.slotCursor();
            nextMemoryBlockIdx = memoryBlockIdx + 1;
            nextMemoryBlock = null;
            memoryBlockIdx++;
            if (advanceToUnmarkedSlot()) {
                return true;
            }
        }
        return false;
    }

    protected boolean advancePartition() {
        if (nextPartitionId > partitions.length - 1) {
            return false;
        }
        for (int partitionId = nextPartitionId; partitionId < partitions.length; partitionId++) {
            memoryBlockIdx = 0;
            partition = partitions[partitionId];
            memoryBlockChain = partition.getMemoryBlockChain();
            if (checkMemoryBlock()) {
                this.nextPartitionId = partitionId + 1;
                return true;
            }
        }
        nextPartitionId = partitions.length;
        return false;
    }

    private MemoryBlock currentMemoryBlock() {
        return nextMemoryBlock == null ? memoryBlock : nextMemoryBlock;
    }

    private void consumeOtherBlocks(long baseTupleAddress, long baseValueAddress, long baseValueSize) {
        for (int blockIdx = memoryBlockIdx; blockIdx < memoryBlockChain.size(); blockIdx++) {
            final MemoryBlock mBlock = memoryBlockChain.get(blockIdx);
            header.setMemoryBlock(mBlock);
            serviceKeyValueStorage.setMemoryBlock(mBlock);
            serviceKeyValueStorage.gotoAddress(header.baseAddress());
            final long slotAddress = serviceKeyValueStorage.addrOfSlotWithSameKey(
                    baseTupleAddress, memoryBlock.getAccessor());
            if (slotAddress == NULL_ADDRESS) {
                continue;
            }
            serviceKeyValueStorage.markSlot(slotAddress, Util.BYTE_1);
            final TupleAddressCursor cursor = serviceKeyValueStorage.tupleCursor(slotAddress);
            consumeTupleCursor(cursor, baseValueAddress, baseValueSize, mBlock);
        }
    }

    private void consumeTupleCursor(
            TupleAddressCursor cursor, long baseValueAddress, long baseValueSize, MemoryBlock mBlock
    ) {
        while (cursor.advance()) {
            final long tupleAddress = cursor.tupleAddress();
            final long valueAddress = JetIoUtil.addrOfValueBlockAt(tupleAddress, mBlock.getAccessor());
            final long valueSize = JetIoUtil.sizeOfValueBlockAt(tupleAddress, mBlock.getAccessor());
            accumulator.accept(memoryBlock.getAccessor(), mBlock.getAccessor(),
                    baseValueAddress, baseValueSize, valueAddress, valueSize, useBigEndian);
        }
    }
}
