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

package com.hazelcast.jet.memory.impl.memory.impl.spilling.spillers;


import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.api.binarystorage.BloomFilter;
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.impl.binarystorage.DefaultStorageHeader;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotIterator;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinaryRecordIterator;

import java.io.File;
import java.util.Arrays;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;


public class KeyValueStorageSpillerImpl<T>
        extends BaseKeyValueSpillerImpl<T, KeyValueDataPartition<T>> {
    private long hashCode;

    private long slotAddress;

    private final int chunkSize;

    private int partitionIdx;

    private int memoryBlockIdx;

    private int nextBlockIndex;

    private long recordAddress;

    private MemoryBlock recordMemoryBlock;

    private MemoryBlock nextRecordMemoryBlock;

    private MemoryBlock memoryBlock;

    private MemoryBlock nextMemoryBlock;

    private KeyValueDataPartition partition;

    private KeyValueDataPartition[] partitions;

    private final BloomFilter bloomFilter;

    private boolean mergingFinished;

    private boolean spilledDataWritten;

    private boolean memoryDataWritten;

    private boolean nextRecordsSpilled;

    private boolean recordsSpilled;

    private boolean slotsLookedUp;

    private StorageHeader storageHeader;

    private boolean hasActiveSpilledSource;

    private final BinaryFunctor binaryFunctor;

    private BinaryKeyValueStorage currentStorage;

    private final MemoryBlock serviceMemoryBlock;

    private BinarySlotIterator binarySlotIterator;

    private BinaryRecordIterator binaryRecordIterator;

    private BinaryRecordIterator binaryRecordNextIterator;

    private long[] lookedUpSlots;

    private int lookedUpSlotIndex;

    public KeyValueStorageSpillerImpl(
            BloomFilter bloomFilter,
            MemoryBlock serviceMemoryBlock,
            BinaryFunctor binaryFunctor,
            int spillingBufferSize,
            int chunkSize,
            File spillingDirectory,
            boolean useBigEndian
    ) {
        super(spillingBufferSize, spillingDirectory, useBigEndian);
        this.chunkSize = chunkSize;
        this.bloomFilter = bloomFilter;
        this.binaryFunctor = binaryFunctor;
        this.serviceMemoryBlock = serviceMemoryBlock;
        this.storageHeader = new DefaultStorageHeader();
    }

    @Override
    public void start(KeyValueDataPartition[] partitions) {
        this.partitions = partitions;

        checkSpillingFile();
        reset();

        try {
            spillingDataInput.open(new FileInputStream(activeSpillingFile));
            spillingDataOutput.open(new FileOutputStream(temporarySpillingFile));
            spillingKeyValueReader.open(spillingDataInput);
            spillingKeyValueWriter.open(spillingDataOutput);
        } catch (FileNotFoundException e) {
            throw Util.reThrow(e);
        }
    }

    @Override
    public boolean processNextChunk() {
        if (!mergingFinished) {
            spillNextChunkForMerge();
            return false;
        }

        return spillNextChunk()
                && checkMemoryBlock()
                && checkPartition();
    }

    private boolean checkMemoryBlock() {
        if (partition == null) {
            return true;
        }

        if (memoryBlock == null) {
            while (memoryBlockIdx < partition.getMemoryBlockChain().size()) {
                memoryBlock =
                        partition.getMemoryBlockChain().getElement(memoryBlockIdx);
                this.storageHeader.setMemoryBlock(memoryBlock);

                if (this.storageHeader.getBaseStorageAddress() != MemoryUtil.NULL_VALUE) {
                    this.currentStorage = partition.getKeyValueStorage();
                    this.currentStorage.setMemoryBlock(memoryBlock);
                    this.currentStorage.gotoAddress(this.storageHeader.getBaseStorageAddress());
                    this.binarySlotIterator = this.currentStorage.slotIterator();
                    memoryBlockIdx++;
                    resetSpillingFlags();
                    nextBlockIndex = memoryBlockIdx;
                    return false;
                }

                memoryBlockIdx++;
            }

            partition = null;
            memoryBlockIdx = 0;
            return true;
        }

        return false;
    }

    private void resetSpillingFlags() {
        recordsSpilled = true;
        nextRecordsSpilled = true;
    }

    private boolean checkPartition() {
        if (partition == null) {
            if (partitionIdx < partitions.length) {
                partition =
                        partitions[partitionIdx];

                if (lookedUpSlots == null) {
                    lookedUpSlots = new long[partition.getMemoryBlockChain().size()];
                }

                if (lookedUpSlots.length < partition.getMemoryBlockChain().size()) {
                    lookedUpSlots = new long[partition.getMemoryBlockChain().size()];
                }

                lookedUpSlotIndex = 0;
                Arrays.fill(lookedUpSlots, MemoryUtil.NULL_VALUE);
            } else {
                memoryBlock = null;
                nextMemoryBlock = null;
                memoryBlockIdx = 0;
                partitionIdx = 0;
                return true;
            }

            partitionIdx++;
        }

        return false;
    }

    private void spillNextChunkForMerge() {
        int spilledNumberCount = 0;

        while (spilledNumberCount < chunkSize) {
            if (!spillCurrentRecordForMerge()) {
                spilledNumberCount++;
                continue;
            }

            if (readSpilledSourceForMerge() &&
                    readSpilledSlotForMerge()) {
                resetVariables();
                mergingFinished = true;
                break;
            }
        }

        if (spilledNumberCount > 0) {
            flushWriter();
        }
    }

    private boolean spillNextChunk() {
        if ((memoryBlock == null)
                || (partition == null)) {
            return true;
        }

        boolean done = false;
        int spilledNumberCount = 0;

        while (spilledNumberCount < chunkSize) {
            if (
                    (spillRecordFromIterator())
                            &&
                            (spillRecordFromNextIterator())
                            &&
                            (checkRecordFromNextBlock())
                            &&
                            (spillSlots())
                    ) {
                done = true;
                memoryBlock = null;
                break;
            }

            spilledNumberCount++;
        }

        if (spilledNumberCount > 0) {
            flushWriter();
        }

        return done;
    }

    private void calculateAndWrite() {
        int blockIndex = nextBlockIndex;

        while (blockIndex < partition.getMemoryBlockChain().size()) {
            MemoryBlock nextMemoryBlock =
                    partition.getMemoryBlockChain().getElement(blockIndex);

            initStorage(nextMemoryBlock);

            if (lookedUpSlots[blockIndex] != MemoryUtil.NULL_VALUE) {
                calculateNextBlock(nextMemoryBlock, lookedUpSlots[blockIndex]);
            }

            blockIndex++;
        }

        spillingKeyValueWriter.writeRecord(
                memoryBlock,
                recordAddress
        );

        initStorage(memoryBlock);
    }

    private void calculateNextBlock(MemoryBlock nextMemoryBlock, long nextSlotAddress) {
        BinaryRecordIterator binaryRecordNextIterator =
                currentStorage.recordIterator(nextSlotAddress);

        if (binaryRecordNextIterator.hasNext()) {
            long nextRecordAddress = binaryRecordNextIterator.next();
            long valueAddress = IOUtil.getValueAddress(recordAddress, memoryBlock);
            long valueSize = IOUtil.getValueWrittenBytes(recordAddress, memoryBlock);
            long nextValueAddress = IOUtil.getValueAddress(nextRecordAddress, nextMemoryBlock);
            long nextValueSize = IOUtil.getValueWrittenBytes(nextRecordAddress, nextMemoryBlock);

            binaryFunctor.processStoredData(
                    memoryBlock,
                    nextMemoryBlock,
                    valueAddress,
                    valueSize,
                    nextValueAddress,
                    nextValueSize,
                    useBigEndian
            );
        }
    }

    private boolean checkRecordFromNextBlock() {
        return recordsSpilled && (
                nextRecordsSpilled
                        || checkFunctorAndAddresses()
                        || checkNextBlocks()
        );
    }

    private boolean checkFunctorAndAddresses() {
        return (hasAssociativeFunctor())
                ||
                (slotAddress == MemoryUtil.NULL_VALUE)
                ||
                (recordAddress == MemoryUtil.NULL_VALUE);
    }

    private boolean checkNextBlocks() {
        while (nextBlockIndex < partition.getMemoryBlockChain().size()) {
            nextMemoryBlock =
                    partition.getMemoryBlockChain().getElement(nextBlockIndex);
            storageHeader.setMemoryBlock(nextMemoryBlock);

            long slotAddress = lookedUpSlots[nextBlockIndex];
            binaryRecordNextIterator = null;

            if ((slotAddress != MemoryUtil.NULL_VALUE)) {
                currentStorage.setMemoryBlock(nextMemoryBlock);
                currentStorage.gotoAddress(storageHeader.getBaseStorageAddress());
                currentStorage.markSlot(slotAddress, Util.BYTE);
                binaryRecordNextIterator =
                        currentStorage.recordIterator(slotAddress);
                nextRecordMemoryBlock = nextMemoryBlock;

                if (binaryRecordNextIterator.hasNext()) {
                    nextBlockIndex++;
                    return false;
                }
                binaryRecordNextIterator = null;
            }

            nextBlockIndex++;
        }

        nextRecordsSpilled = true;
        return true;
    }

    private void flushWriter() {
        try {
            spillingKeyValueWriter.flush();
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }

    private boolean spillCurrentRecordForMerge() {
        if (!hasActiveSpilledSource) {
            return true;
        }

        if (!spilledDataWritten) {
            processSpilledDataForMerge();
            return false;
        }

        if (!memoryDataWritten) {
            processMemoryDataForMerge();
            return false;
        }

        return true;
    }

    private void processSpilledDataForMerge() {
        if (!spilledDataWritten) {
            boolean hasRecord = readSpilledRecordForMerge();

            if (hasRecord) {
                if (!slotsLookedUp) {
                    lookUpSlotsAndWriteSourceHeaderForMerge();
                    binaryRecordNextIterator = null;
                    slotsLookedUp = true;
                    lookedUpSlotIndex = 0;
                }

                writeSpilledRecordForMerge();
            }

            spilledDataWritten = !hasRecord;
        }
    }

    private void lookUpSlotsAndWriteSourceHeaderForMerge() {
        long recordsCount =
                spillingKeyValueReader.getRecordsCount();

        long memoryRecordCount =
                lookUpSlotsOverPartition(
                        0,
                        serviceMemoryBlock,
                        MemoryBlock.TOP_OFFSET
                );

        nextMemoryBlock = null;

        if (memoryRecordCount > 0) {
            recordsCount += memoryRecordCount;
            memoryDataWritten = false;
        } else {
            memoryDataWritten = true;
        }

        spillingKeyValueWriter.writeSourceHeader(
                0,
                recordsCount
        );
    }

    private void processMemoryDataForMerge() {
        if (!memoryDataWritten) {
            memoryDataWritten = spillLookedUpSlotsForMerge();
        }
    }

    private boolean calculateAndSpillForMerge() {
        long recordAddress = spillingKeyValueReader.getRecordAddress();

        //1. Calculate spilled record's value address
        long spilledValueAddress = IOUtil.getValueAddress(
                recordAddress,
                serviceMemoryBlock
        );

        long spilledValueSize = IOUtil.getValueWrittenBytes(
                recordAddress,
                serviceMemoryBlock
        );

        for (int idx = 0; idx < lookedUpSlots.length; idx++) {
            long slotAddress = lookedUpSlots[idx];

            if (slotAddress != MemoryUtil.NULL_VALUE) {
                MemoryBlock memoryBlock =
                        partition.getMemoryBlockChain().getElement(idx);
                storageHeader.setMemoryBlock(memoryBlock);

                if (storageHeader.getBaseStorageAddress() != MemoryUtil.NULL_VALUE) {
                    //2. Apply functor;
                    calculateOverIterator(
                            spilledValueAddress,
                            spilledValueSize,
                            slotAddress,
                            memoryBlock
                    );
                }
            }
        }

        //3. Spill record after calculation;
        spillingKeyValueWriter.writeRecord(
                serviceMemoryBlock,
                MemoryBlock.TOP_OFFSET
        );

        return true;
    }

    private void calculateOverIterator(long spilledValueAddress,
                                       long spilledValueSize,
                                       long slotAddress,
                                       MemoryBlock memoryBlock) {
        currentStorage.setMemoryBlock(memoryBlock);
        currentStorage.gotoAddress(storageHeader.getBaseStorageAddress());
        BinaryRecordIterator iterator =
                currentStorage.recordIterator(slotAddress);

        while (iterator.hasNext()) {
            long recordAddress = iterator.next();
            long valueAddress =
                    IOUtil.getValueAddress(recordAddress, memoryBlock);
            long valueWrittenBytes =
                    IOUtil.getValueWrittenBytes(recordAddress, memoryBlock);
            binaryFunctor.processStoredData(
                    serviceMemoryBlock,
                    memoryBlock.getAccessor(),
                    spilledValueAddress,
                    spilledValueSize,
                    valueAddress,
                    valueWrittenBytes,
                    useBigEndian
            );
        }
    }

    private long lookUpSlotsOverPartition(int startIdx,
                                          MemoryBlock recordMBlock,
                                          long offset) {
        MemoryBlockChain memoryBlockChain =
                partition.getMemoryBlockChain();

        long recordCount = 0;
        Arrays.fill(lookedUpSlots, MemoryUtil.NULL_VALUE);

        for (int index = startIdx; index < memoryBlockChain.size(); index++) {
            MemoryBlock memoryBlock = memoryBlockChain.getElement(index);
            storageHeader.setMemoryBlock(memoryBlock);
            long storageAddress = storageHeader.getBaseStorageAddress();

            if (storageAddress != MemoryUtil.NULL_VALUE) {
                bloomFilter.setMemoryBlock(memoryBlock);
                currentStorage.gotoAddress(storageAddress);
                currentStorage.setMemoryBlock(memoryBlock);

                if (bloomFilter.isSet(hashCode)) {
                    long slotAddress = currentStorage.lookUpSlot(
                            offset,
                            recordMBlock
                    );

                    if (slotAddress != MemoryUtil.NULL_VALUE) {
                        currentStorage.markSlot(slotAddress, Util.BYTE);
                        recordCount += currentStorage.getRecordsCount(slotAddress);
                        lookedUpSlots[index] = slotAddress;
                    }
                }
            }
        }

        return recordCount;
    }

    private boolean hasAssociativeFunctor() {
        return (binaryFunctor != null)
                && (binaryFunctor.isAssociative());
    }


    private boolean spillLookedUpSlotsForMerge() {
        if ((binaryRecordNextIterator == null)
                || (!binaryRecordNextIterator.hasNext())
                || (spillNextRecord())) {
            if (lookedUpSlotIndex >=
                    partition.getMemoryBlockChain().size()) {
                Arrays.fill(lookedUpSlots, MemoryUtil.NULL_VALUE);
                binaryRecordNextIterator = null;
                return true;
            }

            for (int index = lookedUpSlotIndex;
                 index < partition.getMemoryBlockChain().size();
                 index++) {
                long slotAddress = lookedUpSlots[index];

                if (slotAddress != MemoryUtil.NULL_VALUE) {
                    nextMemoryBlock =
                            partition.getMemoryBlockChain().getElement(index);

                    storageHeader.setMemoryBlock(nextMemoryBlock);
                    currentStorage.gotoAddress(
                            storageHeader.getBaseStorageAddress()
                    );

                    currentStorage.setMemoryBlock(nextMemoryBlock);
                    binaryRecordNextIterator =
                            currentStorage.recordIterator(slotAddress);
                    nextRecordMemoryBlock = nextMemoryBlock;

                    if (binaryRecordNextIterator.hasNext()) {
                        spillNextRecord();
                    }

                    lookedUpSlotIndex = index + 1;
                    return false;
                }
            }

            lookedUpSlotIndex = partition.getMemoryBlockChain().size();
        }

        return false;
    }

    protected boolean readSpilledSlotForMerge() {
        if (spillingKeyValueReader.hasNextSlot()) {
            spillingKeyValueReader.readNextSlot();
            partition =
                    partitions[spillingKeyValueReader.getPartitionId()];

            memoryBlockIdx = 0;
            slotsLookedUp = false;
            memoryDataWritten = false;
            spilledDataWritten = false;
            slotAddress = MemoryUtil.NULL_VALUE;
            hashCode = spillingKeyValueReader.getHashCode();

            writeSlotHeader();

            if (hasAssociativeFunctor()) {
                spillingKeyValueReader.readNextSource();
                hasActiveSpilledSource = false;
                spilledDataWritten = true;

                if (!readSpilledRecordForMerge()) {
                    return true;
                }

                lookUpSlotsOverPartition(
                        0,
                        serviceMemoryBlock,
                        MemoryBlock.TOP_OFFSET
                );

                spillingKeyValueWriter.writeSourceHeader(0, 1);
                calculateAndSpillForMerge();
                Arrays.fill(lookedUpSlots, MemoryUtil.NULL_VALUE);
            }

            return false;
        }

        return true;
    }

    protected boolean readSpilledSourceForMerge() {
        if (spillingKeyValueReader.hasNextSource()) {
            spilledDataWritten = false;
            hasActiveSpilledSource = true;
            spillingKeyValueReader.readNextSource();
            return false;
        }

        this.hasActiveSpilledSource = false;
        return true;
    }

    private boolean readSpilledRecordForMerge() {
        if (!spillingKeyValueReader.hasNextRecord()) {
            return false;
        }

        serviceMemoryBlock.reset();
        spillingKeyValueReader.readNextRecord(MemoryBlock.TOP_OFFSET, serviceMemoryBlock);
        return true;
    }

    private void writeSlotHeader() {
        spillingKeyValueWriter.writeSlotHeader(
                spillingKeyValueReader.getPartitionId(),
                spillingKeyValueReader.getHashCode(),
                1
        );
    }

    private void writeSpilledRecordForMerge() {
        long keySize = IOUtil.getKeyWrittenBytes(
                spillingKeyValueReader.getRecordAddress(),
                serviceMemoryBlock
        );
        long valueSize = IOUtil.getValueWrittenBytes(
                spillingKeyValueReader.getRecordAddress(),
                serviceMemoryBlock
        );
        long keyAddress = IOUtil.getKeyAddress(
                spillingKeyValueReader.getRecordAddress(),
                serviceMemoryBlock
        );
        long valueAddress = IOUtil.getValueAddress(
                spillingKeyValueReader.getRecordAddress(),
                serviceMemoryBlock
        );

        spillingKeyValueWriter.writeRecord(
                serviceMemoryBlock,
                keySize,
                valueSize,
                keyAddress,
                valueAddress
        );
    }

    private long readNextUnMarkedSlot() {
        slotAddress = MemoryUtil.NULL_VALUE;
        binaryRecordIterator = null;

        do {
            if (!binarySlotIterator.hasNext()) {
                return MemoryUtil.NULL_VALUE;
            }

            slotAddress = binarySlotIterator.next();
            hashCode = currentStorage.getSlotHashCode(slotAddress);
        } while (currentStorage.getSlotMarker(slotAddress) == Util.BYTE);

        binaryRecordIterator =
                currentStorage.recordIterator(slotAddress);
        recordMemoryBlock = memoryBlock;
        return slotAddress;
    }

    protected boolean spillSlots() {
        if (binarySlotIterator == null) {
            return true;
        }

        initStorage(memoryBlock);

        if (readNextUnMarkedSlot() == MemoryUtil.NULL_VALUE) {
            return true;
        }

        recordsSpilled = false;
        nextRecordsSpilled = false;

        spillingKeyValueWriter.writeSlotHeader(
                partition.getPartitionId(),
                hashCode,
                1
        );

        long headRecordAddress =
                currentStorage.getHeaderRecordAddress(slotAddress);

        long recordsCount = lookUpSlotsOverPartition(
                memoryBlockIdx,
                memoryBlock,
                headRecordAddress
        );

        initStorage(memoryBlock);

        if (hasAssociativeFunctor()) {
            spillingKeyValueWriter.writeSourceHeader(0, 1);
            recordAddress = headRecordAddress;
            calculateAndWrite();
            recordsSpilled = true;
            nextRecordsSpilled = true;
        } else {
            spillingKeyValueWriter.writeSourceHeader(
                    0,
                    recordsCount + currentStorage.getRecordsCount(slotAddress)
            );
            binaryRecordIterator.reset(slotAddress, 0);
        }

        return false;
    }

    private boolean spillRecordFromIterator() {
        return recordsSpilled
                ||
                binaryRecordIterator == null
                ||
                !binaryRecordIterator.hasNext()
                ||
                spillRecord();
    }

    private boolean spillRecordFromNextIterator() {
        if (!recordsSpilled) {
            recordsSpilled = true;
        }

        return nextRecordsSpilled
                || binaryRecordNextIterator == null
                || (!binaryRecordNextIterator.hasNext())
                || spillNextRecord();
    }

    private boolean spillNextRecord() {
        spillingKeyValueWriter.writeRecord(
                nextRecordMemoryBlock,
                binaryRecordNextIterator.next()
        );

        return false;
    }

    private boolean spillRecord() {
        recordAddress =
                binaryRecordIterator.next();

        nextMemoryBlock = null;
        binaryRecordNextIterator = null;
        nextBlockIndex = memoryBlockIdx;

        spillingKeyValueWriter.writeRecord(
                recordMemoryBlock,
                recordAddress
        );

        return false;
    }

    private void initStorage(MemoryBlock memoryBlock) {
        bloomFilter.setMemoryBlock(memoryBlock);
        storageHeader.setMemoryBlock(memoryBlock);
        currentStorage.setMemoryBlock(memoryBlock);
        currentStorage.gotoAddress(storageHeader.getBaseStorageAddress());
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
        recordMemoryBlock = null;
        binarySlotIterator = null;
        binaryRecordIterator = null;
        nextRecordMemoryBlock = null;
        binaryRecordNextIterator = null;
        slotAddress = MemoryUtil.NULL_VALUE;
        recordAddress = MemoryUtil.NULL_VALUE;
    }

    private void resetFlags() {
        resetSpillingFlags();
        this.mergingFinished = false;
        this.memoryDataWritten = false;
        this.spilledDataWritten = false;
        this.hasActiveSpilledSource = false;
    }

    private void reset() {
        resetVariables();
        resetFlags();
    }
}
