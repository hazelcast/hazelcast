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

import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.binarystorage.Storage;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;
import com.hazelcast.jet.memory.binarystorage.cursor.PairAddressCursor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.multimap.PairMultimapHsa;
import com.hazelcast.jet.memory.util.JetIoUtil;
import com.hazelcast.jet.memory.util.Util;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Cursor over in-memory pairs.
 */
public class InMemoryCursor extends PairCursorBase {
    private final Storage serviceKeyValueStorage;
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
    private PairAddressCursor pairCursor;

    public InMemoryCursor(
            Storage serviceKeyValueStorage, MemoryBlock serviceMemoryBlock, MemoryBlock temporaryMemoryBlock,
            Accumulator accumulator, Pair destPair, Partition[] partitions, StorageHeader header,
            SerializationOptimizer optimizer, boolean useBigEndian
    ) {
        super(serviceMemoryBlock, temporaryMemoryBlock, accumulator, destPair, partitions, header, optimizer, useBigEndian);
        this.serviceKeyValueStorage = serviceKeyValueStorage;
    }

    @Override
    public boolean advance() {
        if (!advancePairCursor()) {
            if (!(checkNextMemoryBlock() || advanceSlotCursor() || checkMemoryBlock() || advancePartition())) {
                return false;
            }
            advancePairCursor();
        }
        if (accumulator == null) {
            pairFetcher.fetch(currentMemoryBlock(), pairCursor.pairAddress());
            return true;
        }
        long pairAddress = pairCursor.pairAddress();
        long valueAddress = JetIoUtil.addrOfValueBlockAt(pairAddress, memoryBlock.getAccessor());
        long valueSize = JetIoUtil.sizeOfValueBlockAt(pairAddress, memoryBlock.getAccessor());
        consumePairCursor(pairCursor, valueAddress, valueSize, memoryBlock);
        pairCursor = null;
        consumeOtherBlocks(pairAddress, valueAddress, valueSize);
        pairFetcher.fetch(memoryBlock, pairAddress);
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
        pairCursor = null;
        recordAddress = NULL_ADDRESS;
        super.reset(comparator);
    }

    private boolean advancePairCursor() {
        if (pairCursor != null && pairCursor.advance()) {
            return true;
        }
        pairCursor = null;
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
                long pairAddress = nextMemoryBlock.getAccessor().getLong(slotAddress);
                long pairSize = JetIoUtil.sizeOfPairAt(pairAddress, nextMemoryBlock.getAccessor());
                JetIoUtil.putByte(pairAddress, pairSize + PairMultimapHsa.MARKER_OFFSET, Util.BYTE_1,
                        nextMemoryBlock.getAccessor()
                );
                pairCursor = storage.pairCursor(slotAddress);
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
            recordAddress = storage.addrOfFirstPair(slotAddress);
            if (storage.getSlotMarker(slotAddress) == Util.BYTE_1) {
                continue;
            }
            pairCursor = storage.pairCursor(slotAddress);
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

    private void consumeOtherBlocks(long basePairAddress, long baseValueAddress, long baseValueSize) {
        for (int blockIdx = memoryBlockIdx; blockIdx < memoryBlockChain.size(); blockIdx++) {
            final MemoryBlock mBlock = memoryBlockChain.get(blockIdx);
            header.setMemoryBlock(mBlock);
            serviceKeyValueStorage.setMemoryBlock(mBlock);
            serviceKeyValueStorage.gotoAddress(header.baseAddress());
            final long slotAddress = serviceKeyValueStorage.addrOfSlotWithSameKey(
                    basePairAddress, memoryBlock.getAccessor());
            if (slotAddress == NULL_ADDRESS) {
                continue;
            }
            serviceKeyValueStorage.markSlot(slotAddress, Util.BYTE_1);
            final PairAddressCursor cursor = serviceKeyValueStorage.pairCursor(slotAddress);
            consumePairCursor(cursor, baseValueAddress, baseValueSize, mBlock);
        }
    }

    private void consumePairCursor(
            PairAddressCursor cursor, long baseValueAddress, long baseValueSize, MemoryBlock mBlock
    ) {
        while (cursor.advance()) {
            final long pairAddress = cursor.pairAddress();
            final long valueAddress = JetIoUtil.addrOfValueBlockAt(pairAddress, mBlock.getAccessor());
            final long valueSize = JetIoUtil.sizeOfValueBlockAt(pairAddress, mBlock.getAccessor());
            accumulator.accept(memoryBlock.getAccessor(), mBlock.getAccessor(),
                    baseValueAddress, baseValueSize, valueAddress, valueSize, useBigEndian);
        }
    }
}
