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

package com.hazelcast.jet.memory.impl.operations.aggregator.iterator;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.api.binarystorage.BloomFilter;
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.RecordHeaderLayOut;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotIterator;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinaryRecordIterator;

public class InMemoryIterator<T> extends BaseAggregatorIterator<T> {
    protected long hashCode;
    protected int partitionIdx;
    protected int memoryBlockIdx;
    protected int nextMemoryBlockIdx;
    protected long recordAddress;
    protected MemoryBlock memoryBlock;
    protected MemoryBlock nextMemoryBlock;
    protected KeyValueDataPartition partition;
    protected MemoryBlockChain memoryBlockChain;
    protected BinaryKeyValueStorage keyValueStorage;
    protected BinaryKeyValueStorage serviceKeyValueStorage;
    protected BinarySlotIterator binarySlotIterator;
    protected BinaryRecordIterator binaryRecordIterator;

    public InMemoryIterator(
            BinaryKeyValueStorage serviceKeyValueStorage,
            MemoryBlock serviceMemoryBlock,
            MemoryBlock temporaryMemoryBlock,
            BinaryFunctor functor,
            ContainersPull<T> containersPull,
            KeyValueDataPartition[] partitions,
            StorageHeader header,
            ElementsWriter<T> keysWriter,
            ElementsWriter<T> valuesWriter,
            BloomFilter bloomFilter,
            IOContext ioContext,
            boolean useBigEndian) {
        super(
                serviceMemoryBlock,
                temporaryMemoryBlock,
                functor,
                containersPull,
                partitions,
                header,
                keysWriter,
                valuesWriter,
                bloomFilter,
                ioContext,
                useBigEndian
        );

        this.serviceKeyValueStorage = serviceKeyValueStorage;
    }

    @Override
    protected T readNext() {
        if (checkFunctor()) {
            fetchRecord(memoryBlock, calculate());
        } else {
            long recordAddress = binaryRecordIterator.next();

            fetchRecord(
                    actualMemoryBlock(),
                    recordAddress
            );
        }

        return binaryDataFetcher.getCurrentContainer();
    }

    @Override
    protected boolean checkHasNext() {
        return
                checkRecordIterator()
                        || checkNextMemoryBlock()
                        || checkSlotIterator()
                        || checkMemoryBlock()
                        || checkPartition();
    }

    @Override
    public void reset(BinaryComparator comparator) {
        partitionIdx = 0;
        partition = null;
        memoryBlock = null;
        memoryBlockIdx = 0;
        nextMemoryBlock = null;
        nextMemoryBlockIdx = 0;
        binarySlotIterator = null;
        binaryRecordIterator = null;
        recordAddress = MemoryUtil.NULL_VALUE;
        super.reset(comparator);
    }

    private boolean checkNextMemoryBlock() {
        if (!hashNextBlock()) {
            return false;
        }

        if (checkFunctor()) {
            return false;
        }

        for (int idx = nextMemoryBlockIdx; idx < memoryBlockChain.size(); idx++) {
            nextMemoryBlock = memoryBlockChain.getElement(idx);
            header.setMemoryBlock(memoryBlock);

            if (header.getBaseStorageAddress() == MemoryUtil.NULL_VALUE) {
                continue;
            }

            initMemoryBlock(nextMemoryBlock);

            if (bloomFilter.isSet(hashCode)) {
                long slotAddress =
                        keyValueStorage.lookUpSlot(
                                recordAddress,
                                comparator,
                                memoryBlock
                        );

                if (slotAddress != MemoryUtil.NULL_VALUE) {
                    long recordAddress = nextMemoryBlock.getLong(slotAddress);
                    long recordSize =
                            IOUtil.getRecordSize(recordAddress, nextMemoryBlock);

                    IOUtil.setByte(
                            recordAddress,
                            recordSize + RecordHeaderLayOut.MARKER_OFFSET,
                            Util.BYTE,
                            nextMemoryBlock
                    );

                    binaryRecordIterator =
                            keyValueStorage.recordIterator(slotAddress);
                    nextMemoryBlockIdx = idx + 1;
                    return true;
                }
            }
        }

        header.setMemoryBlock(memoryBlock);
        initMemoryBlock(memoryBlock);
        nextMemoryBlockIdx = memoryBlockChain.size();
        nextMemoryBlock = null;
        return false;
    }

    private boolean hashNextBlock() {
        return memoryBlockChain != null
                &&
                recordAddress != MemoryUtil.NULL_VALUE
                &&
                nextMemoryBlockIdx != 0;
    }

    private void initMemoryBlock(MemoryBlock memoryBlock) {
        bloomFilter.setMemoryBlock(memoryBlock);
        keyValueStorage.setMemoryBlock(memoryBlock);
        keyValueStorage.gotoAddress(header.getBaseStorageAddress());
    }

    protected boolean checkPartition() {
        if (partitionIdx > partitions.length - 1) {
            return false;
        }

        for (int partId = partitionIdx; partId < partitions.length; partId++) {
            memoryBlockIdx = 0;
            KeyValueDataPartition keyValueDataPartition =
                    partitions[partId];
            partition = partitions[partId];
            memoryBlockChain =
                    keyValueDataPartition.getMemoryBlockChain();

            if (checkMemoryBlock()) {
                partitionIdx = partId + 1;
                return true;
            }
        }

        partitionIdx = partitions.length;
        return false;
    }

    private boolean checkRecordIterator() {
        return
                binaryRecordIterator != null
                        && binaryRecordIterator.hasNext();
    }

    private boolean checkSlotIterator() {
        if (binarySlotIterator == null) {
            return false;
        }

        if (processSlotIterator()) {
            return true;
        }

        binaryRecordIterator = null;
        return false;
    }

    private boolean processSlotIterator() {
        while (binarySlotIterator.hasNext()) {
            long slotAddress = binarySlotIterator.next();
            recordAddress = keyValueStorage.getHeaderRecordAddress(slotAddress);

            if (keyValueStorage.getSlotMarker(slotAddress) != Util.BYTE) {
                hashCode = keyValueStorage.getSlotHashCode(slotAddress);
                binaryRecordIterator = keyValueStorage.recordIterator(slotAddress);
                keyValueStorage.markSlot(slotAddress, Util.BYTE);
                nextMemoryBlockIdx = memoryBlockIdx;
                return true;
            }
        }

        return false;
    }

    protected boolean checkMemoryBlock() {
        if (partition == null) {
            return false;
        }

        while (memoryBlockIdx
                <= partition.getMemoryBlockChain().size() - 1) {
            memoryBlock =
                    memoryBlockChain.getElement(memoryBlockIdx);
            keyValueStorage =
                    partitions[partition.getPartitionId()].getKeyValueStorage();

            header.setMemoryBlock(memoryBlock);
            if (header.getBaseStorageAddress() == MemoryUtil.NULL_VALUE) {
                memoryBlockIdx++;
                continue;
            }

            initMemoryBlock(memoryBlock);

            binarySlotIterator = keyValueStorage.slotIterator();
            nextMemoryBlockIdx = memoryBlockIdx + 1;
            nextMemoryBlock = null;
            memoryBlockIdx++;

            if (processSlotIterator()) {
                return true;
            }
        }

        return false;
    }

    private MemoryBlock actualMemoryBlock() {
        return nextMemoryBlock == null
                ?
                memoryBlock
                :
                nextMemoryBlock;
    }

    private long calculate() {
        long baseRecordAddress = binaryRecordIterator.next();
        long baseValueAddress =
                IOUtil.getValueAddress(baseRecordAddress, memoryBlock);
        long baseValueSize =
                IOUtil.getValueWrittenBytes(baseRecordAddress, memoryBlock);

        calculateIterator(
                binaryRecordIterator,
                baseValueSize,
                baseValueAddress,
                memoryBlock
        );

        calculateOverOtherBlocks(
                baseRecordAddress,
                baseValueSize,
                baseValueAddress
        );

        return baseRecordAddress;
    }

    private void calculateOverOtherBlocks(long baseRecordAddress,
                                          long baseValueSize,
                                          long baseValueAddress) {
        for (int blockIdx = memoryBlockIdx;
             blockIdx < memoryBlockChain.size();
             blockIdx++) {
            MemoryBlock mBlock = memoryBlockChain.getElement(blockIdx);
            bloomFilter.setMemoryBlock(mBlock);

            if (bloomFilter.isSet(hashCode)) {
                header.setMemoryBlock(mBlock);

                serviceKeyValueStorage.setMemoryBlock(mBlock);
                serviceKeyValueStorage.gotoAddress(
                        header.getBaseStorageAddress()
                );

                long slotAddress =
                        serviceKeyValueStorage.lookUpSlot(baseRecordAddress, memoryBlock);

                if (slotAddress == MemoryUtil.NULL_VALUE) {
                    continue;
                }

                serviceKeyValueStorage.markSlot(slotAddress, Util.BYTE);
                BinaryRecordIterator iterator = serviceKeyValueStorage.recordIterator(slotAddress);
                calculateIterator(iterator, baseValueSize, baseValueAddress, mBlock);
            }
        }
    }

    private void calculateIterator(BinaryRecordIterator binaryRecordIterator,
                                   long baseValueSize,
                                   long baseValueAddress,
                                   MemoryBlock mBlock) {
        while (binaryRecordIterator.hasNext()) {
            long recordAddress = binaryRecordIterator.next();
            long valueAddress = IOUtil.getValueAddress(recordAddress, mBlock);
            long valueSize = IOUtil.getValueWrittenBytes(recordAddress, mBlock);

            binaryFunctor.processStoredData(
                    memoryBlock,
                    mBlock,
                    baseValueAddress,
                    baseValueSize,
                    valueAddress,
                    valueSize,
                    useBigEndian
            );
        }
    }
}
