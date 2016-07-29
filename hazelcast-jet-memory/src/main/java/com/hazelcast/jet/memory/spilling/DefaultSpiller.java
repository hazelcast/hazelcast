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

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.binarystorage.Storage;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;
import com.hazelcast.jet.memory.binarystorage.cursor.PairAddressCursor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.util.JetIoUtil;
import com.hazelcast.jet.memory.util.Util;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.jet.memory.util.Util.BYTE_1;

/**
 * Default implementation of Spiller.
 */
@SuppressWarnings("checkstyle:methodcount")
public class DefaultSpiller extends SpillerBase implements Spiller {

    private final Accumulator accumulator;
    private final StorageHeader storageHeader;
    private final MemoryBlock serviceMemoryBlock;
    private final int chunkSize;

    private MemoryBlock pairMemoryBlock;
    private MemoryBlock nextPairMemoryBlock;
    private MemoryBlock memoryBlock;
    private MemoryBlock nextMemoryBlock;
    private long hashCode;
    private long slotAddress;
    private int partitionIdx;
    private int memoryBlockIdx;
    private int nextBlockIndex;
    private long pairAddress;
    private Partition partition;
    private Partition[] partitions;

    private boolean haveSpilledDataToWrite = true;
    private boolean haveMemoryDataToWrite = true;
    private boolean havePairToSpill = true;
    private boolean haveNextPairsToSpill = true;
    private boolean haveActiveSpilledSource;
    private boolean slotsLookedUp;
    private boolean mergingDone;

    private Storage currentStorage;
    private SlotAddressCursor slotCursor;
    private PairAddressCursor pairCursor;
    private PairAddressCursor nextPairCursor;
    private long[] addrsOfSlotsWithSameKey;
    private int lookedUpSlotIndex;

    public DefaultSpiller(
            MemoryBlock serviceMemoryBlock, Accumulator accumulator, int spillingBufferSize, int chunkSize,
            File spillingDirectory, boolean useBigEndian
    ) {
        super(spillingDirectory, spillingBufferSize, useBigEndian);
        this.chunkSize = chunkSize;
        this.accumulator = accumulator;
        this.serviceMemoryBlock = serviceMemoryBlock;
        this.storageHeader = new StorageHeader();
    }

    @Override
    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "EI2"},
            justification = "OBL_UNSATISFIED_OBLIGATION: temporarily suppressed, needs inspection;"
                          + "EI2: partition array is not owned by this object")
    public void start(Partition[] partitions) {
        this.partitions = partitions;
        ensureSpillFiles();
        reset();
        try {
            input.setInput(new FileInputStream(activeFile));
            output.setOutput(new FileOutputStream(tempFile));
            spillFileCursor.open(input);
            recordWriter.open(output);
        } catch (FileNotFoundException e) {
            throw Util.rethrow(e);
        }
    }

    @Override
    public boolean processNextChunk() {
        if (!mergingDone) {
            spillNextChunkForMerge();
            return true;
        }
        return spillNextChunk() || findUnprocessedMemoryBlock() || findUnprocessedPartition();
    }

    private boolean findUnprocessedMemoryBlock() {
        if (partition == null) {
            return false;
        }
        if (memoryBlock != null) {
            return true;
        }
        while (memoryBlockIdx < partition.getMemoryBlockChain().size()) {
            memoryBlock = partition.getMemoryBlockChain().get(memoryBlockIdx);
            storageHeader.setMemoryBlock(memoryBlock);
            if (storageHeader.baseAddress() != NULL_ADDRESS) {
                currentStorage = partition.getStorage();
                currentStorage.setMemoryBlock(memoryBlock);
                currentStorage.gotoAddress(storageHeader.baseAddress());
                slotCursor = currentStorage.slotCursor();
                memoryBlockIdx++;
                resetSpillingFlags();
                nextBlockIndex = memoryBlockIdx;
                return true;
            }
            memoryBlockIdx++;
        }
        partition = null;
        memoryBlockIdx = 0;
        return false;
    }

    private boolean findUnprocessedPartition() {
        if (partition != null) {
            return true;
        }
        if (partitionIdx < partitions.length) {
            partition = partitions[partitionIdx];
            if (addrsOfSlotsWithSameKey == null || addrsOfSlotsWithSameKey.length < partition.getMemoryBlockChain().size()) {
                addrsOfSlotsWithSameKey = new long[partition.getMemoryBlockChain().size()];
            }
            lookedUpSlotIndex = 0;
            Arrays.fill(addrsOfSlotsWithSameKey, NULL_ADDRESS);
            partitionIdx++;
            return true;
        }
        memoryBlock = null;
        nextMemoryBlock = null;
        memoryBlockIdx = 0;
        partitionIdx = 0;
        return false;
    }

    private void resetSpillingFlags() {
        havePairToSpill = false;
        haveNextPairsToSpill = false;
    }

    private void spillNextChunkForMerge() {
        int spilledNumberCount = 0;
        while (spilledNumberCount < chunkSize) {
            if (spillCurrentRecordForMerge()) {
                spilledNumberCount++;
                continue;
            }
            if (readSpilledSourceForMerge() && readSpilledSlotForMerge()) {
                resetVariables();
                mergingDone = true;
                break;
            }
        }
        if (spilledNumberCount > 0) {
            flushWriter();
        }
    }

    private boolean spillNextChunk() {
        if (memoryBlock == null || partition == null) {
            return false;
        }
        boolean hasMore = true;
        int spilledCount = 0;
        for (; spilledCount < chunkSize; spilledCount++) {
            if (!spillPairFromCursor()) {
                havePairToSpill = false;
                if (!spillPairFromNextIterator() && !findRecordFromNextBlock() && !spillSlots()) {
                    hasMore = false;
                    memoryBlock = null;
                    break;
                }
            }
        }
        if (spilledCount > 0) {
            flushWriter();
        }
        return hasMore;
    }

    private void accumulateAndWrite() {
        final MemoryBlockChain chain = partition.getMemoryBlockChain();
        for (int blockIndex = nextBlockIndex; blockIndex < chain.size();) {
            MemoryBlock nextBlock = chain.get(blockIndex);
            initStorage(nextBlock);
            final long lookedUpSlot = addrsOfSlotsWithSameKey[blockIndex];
            if (lookedUpSlot != NULL_ADDRESS) {
                accumulate(nextBlock, lookedUpSlot);
            }
            blockIndex++;
        }
        recordWriter.writeRecord(memoryBlock, pairAddress);
        initStorage(memoryBlock);
    }

    private void accumulate(MemoryBlock nextMemoryBlock, long nextSlotAddress) {
        final PairAddressCursor cursor = currentStorage.pairCursor(nextSlotAddress);
        if (!cursor.advance()) {
            return;
        }
        long nextRecordAddress = cursor.pairAddress();
        long valueAddress = JetIoUtil.addrOfValueBlockAt(pairAddress, memoryBlock.getAccessor());
        long valueSize = JetIoUtil.sizeOfValueBlockAt(pairAddress, memoryBlock.getAccessor());
        long nextValueAddress = JetIoUtil.addrOfValueBlockAt(nextRecordAddress, nextMemoryBlock.getAccessor());
        long nextValueSize = JetIoUtil.sizeOfValueBlockAt(nextRecordAddress, nextMemoryBlock.getAccessor());
        accumulator.accept(memoryBlock.getAccessor(), nextMemoryBlock.getAccessor(),
                valueAddress, valueSize, nextValueAddress, nextValueSize, useBigEndian);
    }

    private boolean findNextBlock() {
        while (nextBlockIndex < partition.getMemoryBlockChain().size()) {
            nextMemoryBlock = partition.getMemoryBlockChain().get(nextBlockIndex);
            storageHeader.setMemoryBlock(nextMemoryBlock);
            long slotAddress = addrsOfSlotsWithSameKey[nextBlockIndex];
            nextPairCursor = null;
            if (slotAddress != NULL_ADDRESS) {
                currentStorage.setMemoryBlock(nextMemoryBlock);
                currentStorage.gotoAddress(storageHeader.baseAddress());
                currentStorage.markSlot(slotAddress, BYTE_1);
                nextPairCursor = currentStorage.pairCursor(slotAddress);
                nextPairMemoryBlock = nextMemoryBlock;
                if (nextPairCursor.advance()) {
                    nextBlockIndex++;
                    return true;
                }
                nextPairCursor = null;
            }
            nextBlockIndex++;
        }
        haveNextPairsToSpill = false;
        return false;
    }

    private void flushWriter() {
        try {
            recordWriter.flush();
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }

    private boolean spillCurrentRecordForMerge() {
        if (!haveActiveSpilledSource) {
            return false;
        }
        if (haveSpilledDataToWrite) {
            processSpilledDataForMerge();
            return true;
        }
        if (haveMemoryDataToWrite) {
            haveMemoryDataToWrite = !spillLookedUpSlotsForMerge();
            return true;
        }
        return false;
    }

    private void processSpilledDataForMerge() {
        boolean hasRecord = readSpilledRecordForMerge();
        if (hasRecord) {
            ensureSlotsLookedUp();
            writeSpilledRecordForMerge();
        }
        haveSpilledDataToWrite = hasRecord;
    }

    private void ensureSlotsLookedUp() {
        if (slotsLookedUp) {
            return;
        }
        long recordsCount = spillFileCursor.getRecordCountInCurrentSegment();
        long memoryRecordCount = findPairsInCurrentPartitionWithSameKey(0, serviceMemoryBlock, MemoryBlock.TOP_OFFSET);
        nextMemoryBlock = null;
        if (memoryRecordCount > 0) {
            recordsCount += memoryRecordCount;
            haveMemoryDataToWrite = true;
        } else {
            haveMemoryDataToWrite = false;
        }
        recordWriter.writeSegmentHeader(0, recordsCount);
        nextPairCursor = null;
        slotsLookedUp = true;
        lookedUpSlotIndex = 0;
    }

    private boolean calculateAndSpillForMerge() {
        long recordAddress = spillFileCursor.getRecordAddress();

        //1. Calculate spilled record's value address
        long spilledValueAddress = JetIoUtil.addrOfValueBlockAt(recordAddress, serviceMemoryBlock.getAccessor());
        long spilledValueSize = JetIoUtil.sizeOfValueBlockAt(recordAddress, serviceMemoryBlock.getAccessor());
        for (int idx = 0; idx < addrsOfSlotsWithSameKey.length; idx++) {
            long slotAddress = addrsOfSlotsWithSameKey[idx];
            if (slotAddress != NULL_ADDRESS) {
                MemoryBlock memoryBlock = partition.getMemoryBlockChain().get(idx);
                storageHeader.setMemoryBlock(memoryBlock);
                if (storageHeader.baseAddress() != NULL_ADDRESS) {
                    //2. Apply accumulator
                    accumulateOverIterator(spilledValueAddress, spilledValueSize, slotAddress, memoryBlock);
                }
            }
        }

        //3. Spill record after calculation;
        recordWriter.writeRecord(serviceMemoryBlock, MemoryBlock.TOP_OFFSET);
        return true;
    }

    private void accumulateOverIterator(
            long spilledValueAddress, long spilledValueSize, long slotAddress, MemoryBlock memoryBlock
    ) {
        currentStorage.setMemoryBlock(memoryBlock);
        currentStorage.gotoAddress(storageHeader.baseAddress());
        for (PairAddressCursor cursor = currentStorage.pairCursor(slotAddress); cursor.advance();) {
            long recordAddress = cursor.pairAddress();
            MemoryAccessor accessor = memoryBlock.getAccessor();
            long valueAddress = JetIoUtil.addrOfValueBlockAt(recordAddress, accessor);
            long valueWrittenBytes = JetIoUtil.sizeOfValueBlockAt(recordAddress, accessor);
            accumulator.accept(serviceMemoryBlock.getAccessor(), accessor,
                    spilledValueAddress, spilledValueSize, valueAddress, valueWrittenBytes, useBigEndian);
        }
    }

    private long findPairsInCurrentPartitionWithSameKey(
            int startBlockIdx, MemoryBlock pairMemBlock, long pairAddress) {
        final MemoryBlockChain chain = partition.getMemoryBlockChain();
        long pairCount = 0;
        Arrays.fill(addrsOfSlotsWithSameKey, NULL_ADDRESS);
        for (int blockIdx = startBlockIdx; blockIdx < chain.size(); blockIdx++) {
            final MemoryBlock mBlock = chain.get(blockIdx);
            storageHeader.setMemoryBlock(mBlock);
            long storageAddress = storageHeader.baseAddress();
            if (storageAddress == NULL_ADDRESS) {
                continue;
            }
            currentStorage.gotoAddress(storageAddress);
            currentStorage.setMemoryBlock(mBlock);
            long slotAddress = currentStorage.addrOfSlotWithSameKey(pairAddress, pairMemBlock.getAccessor());
            if (slotAddress == NULL_ADDRESS) {
                continue;
            }
            currentStorage.markSlot(slotAddress, BYTE_1);
            pairCount += currentStorage.pairCountAt(slotAddress);
            addrsOfSlotsWithSameKey[blockIdx] = slotAddress;
        }
        return pairCount;
    }

    private boolean spillLookedUpSlotsForMerge() {
        spillNextPair();
        if (nextPairCursor != null) {
            return false;
        }
        if (lookedUpSlotIndex >= partition.getMemoryBlockChain().size()) {
            Arrays.fill(addrsOfSlotsWithSameKey, NULL_ADDRESS);
            nextPairCursor = null;
            return true;
        }
        for (int index = lookedUpSlotIndex;
             index < partition.getMemoryBlockChain().size();
             index++) {
            long slotAddress = addrsOfSlotsWithSameKey[index];
            if (slotAddress == NULL_ADDRESS) {
                continue;
            }
            nextMemoryBlock = partition.getMemoryBlockChain().get(index);
            storageHeader.setMemoryBlock(nextMemoryBlock);
            currentStorage.gotoAddress(storageHeader.baseAddress());
            currentStorage.setMemoryBlock(nextMemoryBlock);
            nextPairCursor = currentStorage.pairCursor(slotAddress);
            nextPairMemoryBlock = nextMemoryBlock;
            if (nextPairCursor.advance()) {
                spillNextPair();
            } else {
                nextPairCursor = null;
            }
            lookedUpSlotIndex = index + 1;
            return false;
        }
        lookedUpSlotIndex = partition.getMemoryBlockChain().size();
        return false;
    }

    protected boolean readSpilledSlotForMerge() {
        if (!spillFileCursor.slotAdvance()) {
            return true;
        }
        partition = partitions[spillFileCursor.getPartitionId()];
        memoryBlockIdx = 0;
        slotsLookedUp = false;
        haveMemoryDataToWrite = true;
        haveSpilledDataToWrite = true;
        slotAddress = NULL_ADDRESS;
        hashCode = spillFileCursor.getHashCode();
        recordWriter.writeSlotHeader(spillFileCursor.getPartitionId(), spillFileCursor.getHashCode(), 1);
        if (!hasAssociativeAccumulator()) {
            return false;
        }
        spillFileCursor.segmentAdvance();
        haveActiveSpilledSource = false;
        haveSpilledDataToWrite = false;
        if (!readSpilledRecordForMerge()) {
            return true;
        }
        findPairsInCurrentPartitionWithSameKey(0, serviceMemoryBlock, MemoryBlock.TOP_OFFSET);
        recordWriter.writeSegmentHeader(0, 1);
        calculateAndSpillForMerge();
        Arrays.fill(addrsOfSlotsWithSameKey, NULL_ADDRESS);
        return false;
    }

    protected boolean readSpilledSourceForMerge() {
        if (spillFileCursor.segmentAdvance()) {
            haveSpilledDataToWrite = true;
            haveActiveSpilledSource = true;
            return false;
        }
        this.haveActiveSpilledSource = false;
        return true;
    }

    private boolean readSpilledRecordForMerge() {
        return spillFileCursor.recordAdvance(serviceMemoryBlock, MemoryBlock.TOP_OFFSET, true);
    }

    private void writeSpilledRecordForMerge() {
        final MemoryAccessor accessor = serviceMemoryBlock.getAccessor();
        final long recAddr = spillFileCursor.getRecordAddress();
        long keySize = JetIoUtil.sizeOfKeyBlockAt(recAddr, accessor);
        long valueSize = JetIoUtil.sizeOfValueBlockAt(recAddr, accessor);
        long keyAddress = JetIoUtil.addressOfKeyBlockAt(recAddr);
        long valueAddress = JetIoUtil.addrOfValueBlockAt(recAddr, accessor);
        recordWriter.writeRecord(serviceMemoryBlock, keySize, valueSize, keyAddress, valueAddress);
    }

    private long gotoNextUnmarkedSlot() {
        slotAddress = NULL_ADDRESS;
        pairCursor = null;
        do {
            if (!slotCursor.advance()) {
                return NULL_ADDRESS;
            }
            slotAddress = slotCursor.slotAddress();
            hashCode = currentStorage.getSlotHashCode(slotAddress);
        } while (currentStorage.getSlotMarker(slotAddress) == BYTE_1);
        pairCursor = currentStorage.pairCursor(slotAddress);
        pairMemoryBlock = memoryBlock;
        return slotAddress;
    }

    protected boolean spillSlots() {
        if (slotCursor == null) {
            return false;
        }
        initStorage(memoryBlock);
        if (gotoNextUnmarkedSlot() == NULL_ADDRESS) {
            return false;
        }
        recordWriter.writeSlotHeader(partition.getPartitionId(), hashCode, 1);
        long addrOfFirstPair = currentStorage.addrOfFirstPair(slotAddress);
        long pairCount = findPairsInCurrentPartitionWithSameKey(memoryBlockIdx, memoryBlock, addrOfFirstPair);
        initStorage(memoryBlock);
        if (hasAssociativeAccumulator()) {
            havePairToSpill = false;
            haveNextPairsToSpill = false;
            recordWriter.writeSegmentHeader(0, 1);
            pairAddress = addrOfFirstPair;
            accumulateAndWrite();
        } else {
            havePairToSpill = true;
            haveNextPairsToSpill = true;
            recordWriter.writeSegmentHeader(0, pairCount + currentStorage.pairCountAt(slotAddress));
            pairCursor.reset(slotAddress, 0);
        }
        return true;
    }

    private boolean spillPairFromCursor() {
        if (!havePairToSpill || pairCursor == null || !pairCursor.advance()) {
            pairCursor = null;
            return false;
        }
        pairAddress = pairCursor.pairAddress();
        nextMemoryBlock = null;
        nextPairCursor = null;
        nextBlockIndex = memoryBlockIdx;
        recordWriter.writeRecord(pairMemoryBlock, pairAddress);
        return true;
    }

    private boolean spillPairFromNextIterator() {
        if (haveNextPairsToSpill && nextPairCursor != null) {
            spillNextPair();
            return true;
        }
        return false;
    }

    private void spillNextPair() {
        recordWriter.writeRecord(nextPairMemoryBlock, nextPairCursor.pairAddress());
    }

    private boolean findRecordFromNextBlock() {
        return haveNextPairsToSpill && checkAccumulatorAndAddresses() && findNextBlock();
    }

    private boolean checkAccumulatorAndAddresses() {
        return !hasAssociativeAccumulator() && slotAddress != NULL_ADDRESS && pairAddress != NULL_ADDRESS;
    }

    private boolean hasAssociativeAccumulator() {
        return accumulator != null && accumulator.isAssociative();
    }

    private void initStorage(MemoryBlock memoryBlock) {
        storageHeader.setMemoryBlock(memoryBlock);
        currentStorage.setMemoryBlock(memoryBlock);
        currentStorage.gotoAddress(storageHeader.baseAddress());
    }

    private void resetVariables() {
        hashCode = 0L;
        partitionIdx = 0;
        nextBlockIndex = 0;
        memoryBlockIdx = 0;
        partition = null;
        memoryBlock = null;
        slotsLookedUp = false;
        nextMemoryBlock = null;
        pairMemoryBlock = null;
        slotCursor = null;
        pairCursor = null;
        nextPairMemoryBlock = null;
        nextPairCursor = null;
        slotAddress = NULL_ADDRESS;
        pairAddress = NULL_ADDRESS;
    }

    private void resetFlags() {
        resetSpillingFlags();
        this.mergingDone = false;
        this.haveMemoryDataToWrite = true;
        this.haveSpilledDataToWrite = true;
        this.haveActiveSpilledSource = false;
    }

    private void reset() {
        resetVariables();
        resetFlags();
    }

}
