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

import java.util.Arrays;

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
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinaryRecordIterator;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueReader;
import com.hazelcast.jet.memory.api.memory.spilling.spillers.KeyValueStorageSpiller;


public class SpillingIterator<T, P extends KeyValueDataPartition<T>>
        extends BaseAggregatorIterator<T> {
    private long hashCode;
    private int lookedUpSlotIdx;
    private long[] lookedUpSlots;
    private MemoryBlock memoryBlock;
    private boolean pendingSpilledSlot;
    private BinaryKeyValueStorage storage;
    private MemoryBlockChain memoryBlockChain;
    private final KeyValueStorageSpiller<T, P> spiller;
    private BinaryRecordIterator binaryRecordIterator;
    private SpillingKeyValueReader spillingKeyValueReader;

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public SpillingIterator(
            MemoryBlock serviceMemoryBlock,
            MemoryBlock temporaryMemoryBlock,
            BinaryFunctor functor,
            KeyValueStorageSpiller<T, P> spiller,
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

        this.spiller = spiller;
    }

    @Override
    protected boolean checkHasNext() {
        return
                checkSpilledRecords()
                        ||
                        checkLookedUpSlot()
                        ||
                        checkSpilledSlot();
    }

    @Override
    public T readNext() {
        hasNextCalled = false;
        return binaryDataFetcher.getCurrentContainer();
    }

    @Override
    public void reset(BinaryComparator comparator) {
        super.reset(comparator);

        storage = null;
        memoryBlock = null;
        hasNextCalled = false;
        memoryBlockChain = null;
        lastHasNextResult = false;
        pendingSpilledSlot = false;
        spillingKeyValueReader =
                spiller.openSpillingReader();
    }

    private boolean checkLookedUpSlot() {
        if (!pendingSpilledSlot) {
            return false;
        }

        if (processRecordIterator()) {
            return true;
        }

        if (lookedUpSlotIdx
                >= memoryBlockChain.size()) {
            pendingSpilledSlot = false;
            return false;
        }

        for (int idx = lookedUpSlotIdx; idx < memoryBlockChain.size(); idx++) {
            long lookedUpSlot = lookedUpSlots[idx];

            if (lookedUpSlot != MemoryUtil.NULL_VALUE) {
                memoryBlock = memoryBlockChain.getElement(idx);
                storage.setMemoryBlock(memoryBlock);
                binaryRecordIterator =
                        storage.recordIterator(lookedUpSlot);
                lookedUpSlotIdx = idx + 1;
                return processRecordIterator();
            }
        }

        pendingSpilledSlot = false;
        lookedUpSlotIdx = memoryBlockChain.size();

        return false;
    }

    private boolean processRecordIterator() {
        if ((binaryRecordIterator != null)
                && (binaryRecordIterator.hasNext())) {
            long recordAddress =
                    binaryRecordIterator.next();

            fetchRecord(
                    memoryBlock,
                    recordAddress
            );

            return true;
        }

        return false;
    }

    private void lookUpOverMemoryBlocks() {
        lookedUpSlotIdx = 0;

        for (int idx = 0; idx < memoryBlockChain.size(); idx++) {
            MemoryBlock mBlock = memoryBlockChain.getElement(idx);
            header.setMemoryBlock(mBlock);

            if (header.getBaseStorageAddress() == MemoryUtil.NULL_VALUE) {
                continue;
            }

            bloomFilter.setMemoryBlock(mBlock);

            if (bloomFilter.isSet(hashCode)) {
                storage.setMemoryBlock(mBlock);
                storage.gotoAddress(header.getBaseStorageAddress());

                long slotAddress = storage.lookUpSlot(
                        MemoryBlock.TOP_OFFSET,
                        serviceMemoryBlock
                );

                if (slotAddress != MemoryUtil.NULL_VALUE) {
                    lookedUpSlots[idx] = slotAddress;
                    storage.markSlot(slotAddress, Util.BYTE);
                }
            }
        }
    }

    private boolean checkSpilledRecords() {
        if (spillingKeyValueReader.hasNextRecord()) {
            serviceMemoryBlock.reset();
            spillingKeyValueReader.readNextRecord(MemoryBlock.TOP_OFFSET, serviceMemoryBlock);
            fetchRecord(serviceMemoryBlock, MemoryBlock.TOP_OFFSET);
            return true;
        }

        return false;
    }

    private void calculateSlotData() {
        serviceMemoryBlock.reset();
        spillingKeyValueReader.readNextRecord(MemoryBlock.TOP_OFFSET, serviceMemoryBlock);

        long oldValueAddress =
                IOUtil.getValueAddress(MemoryBlock.TOP_OFFSET, serviceMemoryBlock);

        long oldValueSize =
                IOUtil.getValueWrittenBytes(MemoryBlock.TOP_OFFSET, serviceMemoryBlock);

        while (spillingKeyValueReader.hasNextRecord()) {
            spillingKeyValueReader.readNextRecord(MemoryBlock.TOP_OFFSET, temporaryMemoryBlock);

            long newValueAddress =
                    IOUtil.getValueAddress(MemoryBlock.TOP_OFFSET, temporaryMemoryBlock);
            long newValueSize =
                    IOUtil.getValueWrittenBytes(MemoryBlock.TOP_OFFSET, temporaryMemoryBlock);

            binaryFunctor.processStoredData(
                    serviceMemoryBlock,
                    temporaryMemoryBlock,
                    oldValueAddress,
                    oldValueSize,
                    newValueAddress,
                    newValueSize,
                    useBigEndian
            );
        }

        lookUpOverMemoryBlocks();
        calculateOverPartition(
                oldValueAddress,
                oldValueSize
        );
    }

    private void calculateOverPartition(long oldValueAddress,
                                        long oldValueSize) {
        for (int idx = 0; idx < lookedUpSlots.length; idx++) {
            long slotAddress = lookedUpSlots[idx];

            if (slotAddress == MemoryUtil.NULL_VALUE) {
                continue;
            }

            MemoryBlock memoryBlock = memoryBlockChain.getElement(idx);
            header.setMemoryBlock(memoryBlock);
            storage.setMemoryBlock(memoryBlock);
            storage.gotoAddress(header.getBaseStorageAddress());
            BinaryRecordIterator iterator = storage.recordIterator(slotAddress);

            while (iterator.hasNext()) {
                long recordAddress = iterator.next();

                long newValueAddress =
                        IOUtil.getValueAddress(recordAddress, memoryBlock);
                long newValueSize =
                        IOUtil.getValueWrittenBytes(recordAddress, memoryBlock);

                binaryFunctor.processStoredData(
                        serviceMemoryBlock,
                        memoryBlock,
                        oldValueAddress,
                        oldValueSize,
                        newValueAddress,
                        newValueSize,
                        useBigEndian
                );
            }
        }
    }

    private boolean checkSpilledSource() {
        if (spillingKeyValueReader.hasNextSource()) {
            spillingKeyValueReader.readNextSource();
            return true;
        }

        return false;
    }

    private boolean checkSpilledSlot() {
        if (spillingKeyValueReader.hasNextSlot()) {
            spillingKeyValueReader.readNextSlot();
            int partitionId = spillingKeyValueReader.getPartitionId();
            memoryBlockChain = partitions[partitionId].getMemoryBlockChain();
            storage = partitions[partitionId].getKeyValueStorage();
            lookedUpSlotIdx = 0;

            if ((lookedUpSlots == null)
                    ||
                    (lookedUpSlots.length < memoryBlockChain.size())
                    ) {
                lookedUpSlots = new long[memoryBlockChain.size()];
            }

            Arrays.fill(lookedUpSlots, MemoryUtil.NULL_VALUE);

            hashCode = spillingKeyValueReader.getHashCode();
            pendingSpilledSlot = checkSpilledSource()
                    &&
                    lookUpOrCalculate();

            return true;
        }

        return false;
    }

    private boolean lookUpOrCalculate() {
        if (checkFunctor()) {
            calculateSlotData();
            fetchRecord(
                    serviceMemoryBlock,
                    MemoryBlock.TOP_OFFSET
            );
            return false;
        } else {
            if (checkSpilledRecords()) {
                lookUpOverMemoryBlocks();
                return true;
            }
            return false;
        }
    }
}
