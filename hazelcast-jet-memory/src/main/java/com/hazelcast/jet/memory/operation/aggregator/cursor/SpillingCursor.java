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

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.binarystorage.Storage;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.cursor.TupleAddressCursor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.spilling.SpillFileCursor;
import com.hazelcast.jet.memory.spilling.Spiller;
import com.hazelcast.jet.memory.util.Util;

import java.util.Arrays;

import static com.hazelcast.jet.memory.memoryblock.MemoryBlock.TOP_OFFSET;
import static com.hazelcast.jet.memory.util.JetIoUtil.addrOfValueBlockAt;
import static com.hazelcast.jet.memory.util.JetIoUtil.sizeOfValueBlockAt;

/**
 * Cursor over tuples that were spilled to disk.
 */
public class SpillingCursor extends TupleCursorBase {
    private final Spiller spiller;
    private long[] lookedUpSlots;
    private int lookedUpSlotIdx;
    private long hashCode;
    private boolean hasPendingSpilledSlot;
    private MemoryBlock memoryBlock;
    private MemoryBlockChain memoryBlockChain;
    private Storage storage;
    private TupleAddressCursor tupleCursor;
    private SpillFileCursor spillFileCursor;

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public SpillingCursor(
            MemoryBlock serviceMemoryBlock, MemoryBlock temporaryMemoryBlock, Accumulator accumulator,
            Spiller spiller, Tuple2 destTuple, Partition[] partitions, StorageHeader header, IOContext ioContext,
            boolean useBigEndian
    ) {
        super(serviceMemoryBlock, temporaryMemoryBlock, accumulator, destTuple, partitions, header, ioContext,
                useBigEndian);
        this.spiller = spiller;
    }

    @Override
    public boolean advance() {
        return checkSpilledTuples() || checkLookedUpSlot() || checkSpilledSlot();
    }

    @Override
    public void reset(Comparator comparator) {
        super.reset(comparator);
        storage = null;
        memoryBlock = null;
        memoryBlockChain = null;
        hasPendingSpilledSlot = false;
        spillFileCursor = spiller.openSpillFileCursor();
    }

    private boolean checkSpilledTuples() {
        if (spillFileCursor.recordAdvance(serviceMemoryBlock, TOP_OFFSET, true)) {
            tupleFetcher.fetch(serviceMemoryBlock, TOP_OFFSET);
            return true;
        }
        return false;
    }

    private boolean checkLookedUpSlot() {
        if (!hasPendingSpilledSlot) {
            return false;
        }
        if (advanceTupleCursor()) {
            return true;
        }
        if (lookedUpSlotIdx >= memoryBlockChain.size()) {
            hasPendingSpilledSlot = false;
            return false;
        }
        for (int idx = lookedUpSlotIdx; idx < memoryBlockChain.size(); idx++) {
            long lookedUpSlot = lookedUpSlots[idx];
            if (lookedUpSlot == MemoryAllocator.NULL_ADDRESS) {
                continue;
            }
            memoryBlock = memoryBlockChain.get(idx);
            storage.setMemoryBlock(memoryBlock);
            tupleCursor = storage.tupleCursor(lookedUpSlot);
            lookedUpSlotIdx = idx + 1;
            return advanceTupleCursor();
        }
        hasPendingSpilledSlot = false;
        lookedUpSlotIdx = memoryBlockChain.size();
        return false;
    }

    private boolean checkSpilledSlot() {
        if (!spillFileCursor.slotAdvance()) {
            return false;
        }
        int partitionId = spillFileCursor.getPartitionId();
        memoryBlockChain = partitions[partitionId].getMemoryBlockChain();
        storage = partitions[partitionId].getStorage();
        lookedUpSlotIdx = 0;
        if (lookedUpSlots == null || lookedUpSlots.length < memoryBlockChain.size()) {
            lookedUpSlots = new long[memoryBlockChain.size()];
        }
        Arrays.fill(lookedUpSlots, MemoryAllocator.NULL_ADDRESS);
        hashCode = spillFileCursor.getHashCode();
        hasPendingSpilledSlot = spillFileCursor.segmentAdvance() && lookUpOrCalculate();
        return true;
    }

    private void lookUpOverMemoryBlocks() {
        lookedUpSlotIdx = 0;
        for (int idx = 0; idx < memoryBlockChain.size(); idx++) {
            MemoryBlock mBlock = memoryBlockChain.get(idx);
            header.setMemoryBlock(mBlock);
            if (header.baseAddress() == MemoryAllocator.NULL_ADDRESS) {
                continue;
            }
            storage.setMemoryBlock(mBlock);
            storage.gotoAddress(header.baseAddress());
            long slotAddress = storage.addrOfSlotWithSameKey(TOP_OFFSET, serviceMemoryBlock.getAccessor());
            if (slotAddress == MemoryAllocator.NULL_ADDRESS) {
                continue;
            }
            lookedUpSlots[idx] = slotAddress;
            storage.markSlot(slotAddress, Util.BYTE_1);
        }
    }

    private void calculateSlotData() {
        spillFileCursor.recordAdvance(serviceMemoryBlock, TOP_OFFSET, true);
        final MemoryAccessor srvAccessor = serviceMemoryBlock.getAccessor();
        long oldValueAddress = addrOfValueBlockAt(TOP_OFFSET, srvAccessor);
        long oldValueSize = sizeOfValueBlockAt(TOP_OFFSET, srvAccessor);
        while (spillFileCursor.recordAdvance(temporaryMemoryBlock, TOP_OFFSET, false)) {
            final MemoryAccessor tmpAccessor = temporaryMemoryBlock.getAccessor();
            long newValueAddress = addrOfValueBlockAt(TOP_OFFSET, tmpAccessor);
            long newValueSize = sizeOfValueBlockAt(TOP_OFFSET, tmpAccessor);
            accumulator.accept(srvAccessor, tmpAccessor, oldValueAddress, oldValueSize,
                    newValueAddress, newValueSize, useBigEndian);
        }
        lookUpOverMemoryBlocks();
        calculateOverPartition(oldValueAddress, oldValueSize);
    }

    private void calculateOverPartition(long oldValueAddress, long oldValueSize) {
        for (int idx = 0; idx < lookedUpSlots.length; idx++) {
            long slotAddress = lookedUpSlots[idx];
            if (slotAddress == MemoryAllocator.NULL_ADDRESS) {
                continue;
            }
            MemoryBlock memoryBlock = memoryBlockChain.get(idx);
            header.setMemoryBlock(memoryBlock);
            storage.setMemoryBlock(memoryBlock);
            storage.gotoAddress(header.baseAddress());
            for (TupleAddressCursor cursor = storage.tupleCursor(slotAddress); cursor.advance();) {
                long recordAddress = cursor.tupleAddress();
                final MemoryAccessor accessor = memoryBlock.getAccessor();
                long newValueAddress = addrOfValueBlockAt(recordAddress, accessor);
                long newValueSize = sizeOfValueBlockAt(recordAddress, accessor);
                accumulator.accept(serviceMemoryBlock.getAccessor(), accessor,
                        oldValueAddress, oldValueSize, newValueAddress, newValueSize, useBigEndian);
            }
        }
    }

    private boolean advanceTupleCursor() {
        if (tupleCursor != null && tupleCursor.advance()) {
            tupleFetcher.fetch(memoryBlock, tupleCursor.tupleAddress());
            return true;
        }
        tupleCursor = null;
        return false;
    }

    private boolean lookUpOrCalculate() {
        if (accumulator != null) {
            calculateSlotData();
            tupleFetcher.fetch(serviceMemoryBlock, TOP_OFFSET);
            return false;
        } else {
            if (checkSpilledTuples()) {
                lookUpOverMemoryBlocks();
                return true;
            }
            return false;
        }
    }
}
