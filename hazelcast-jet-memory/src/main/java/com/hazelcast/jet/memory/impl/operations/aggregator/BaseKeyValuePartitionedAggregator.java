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

package com.hazelcast.jet.memory.impl.operations.aggregator;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsReader;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.api.memory.OutOfMemoryException;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.operations.aggregator.Aggregator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.spi.operations.aggregator.AggregatorState;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.RecordHeaderLayOut;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;

import java.io.IOException;

public abstract class BaseKeyValuePartitionedAggregator<T, E extends KeyValueDataPartition<T>>
        extends BaseKeyValuePartitionedOperator<T, E> implements Aggregator<T> {
    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public BaseKeyValuePartitionedAggregator(int partitionCount,
                                             int spillingBufferSize,
                                             IOContext ioContext,
                                             BinaryComparator binaryComparator,
                                             MemoryContext memoryContext,
                                             MemoryChainingType memoryChainingType,
                                             ElementsWriter<T> keyWriter,
                                             ElementsWriter<T> valueWriter,
                                             ContainersPull<T> containersPull,
                                             BinaryFunctor binaryFunctor,
                                             String spillingDirectory,
                                             int spillingChunkSize,
                                             boolean spillToDisk,
                                             boolean useBigEndian) {
        super(
                partitionCount,
                spillingBufferSize,
                ioContext,
                binaryComparator,
                memoryContext,
                memoryChainingType,
                keyWriter,
                valueWriter,
                containersPull,
                binaryFunctor,
                spillingDirectory,
                spillingChunkSize,
                spillToDisk,
                useBigEndian
        );
    }

    @Override
    public boolean putRecord(T record,
                             ElementsReader<T> keyReader,
                             ElementsReader<T> valueReader) throws IOException {
        try {
            try {
                writeRecord(record, getComparator(), keyReader, valueReader);
            } catch (OutOfMemoryException exception) {
                MemoryBlockChain memoryBlockChain =
                        currentPartition.getMemoryBlockChain();

                if (
                        (!memoryBlockChain.stepNoNext()) &&
                                (!acquireNextBlock(memoryBlockChain))) {
                    return false;
                }

                activateMemoryBlock(currentPartition, memoryBlockChain.activeElement());
                writeRecord(record, getComparator(), keyReader, valueReader);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    private boolean acquireNextBlock(MemoryBlockChain memoryBlockChain) {
        try {
            memoryBlockChain.acquireNextBlock(true);
        } catch (OutOfMemoryException e) {
            if (!spillToDisk) {
                throw new IllegalStateException(
                        "Not enough memory for processing. " +
                                "Spilling is turned off"
                );
            }

            //Need to start spilling
            return false;
        }

        return true;
    }

    private void writeRecord(T record,
                             BinaryComparator binaryComparator,
                             ElementsReader<T> keyReader,
                             ElementsReader<T> valueReader) throws IOException {
        serviceMemoryBlock.reset();

        IOUtil.writeRecord(
                record,
                keyReader,
                valueReader,
                ioContext,
                jetDataOutput,
                serviceMemoryBlock
        );

        long tmpRecordAddress = jetDataOutput.getPointer();

        long hashCode = binaryComparator.getPartitionHasher().hash(
                IOUtil.getKeyAddress(tmpRecordAddress, serviceMemoryBlock),
                IOUtil.getKeyWrittenBytes(tmpRecordAddress, serviceMemoryBlock),
                serviceMemoryBlock
        );

        long recordSize = jetDataOutput.getWrittenSize();
        int partitionNumber = (int) ((Math.abs(hashCode)) & partitionCountBase);

        currentPartition = partitions[partitionNumber];
        BinaryKeyValueStorage<T> binaryKeyValueStorage =
                currentPartition.getKeyValueStorage();
        MemoryBlock memoryBlock =
                currentPartition.getMemoryBlockChain().activeElement();
        header.setMemoryBlock(memoryBlock);
        binaryKeyValueStorage.setMemoryBlock(
                memoryBlock
        );
        jetDataOutput.setMemoryManager(memoryBlock);

        onMemoryBlockSwitched(memoryBlock);

        long recordAddress = memoryBlock.allocate(
                recordSize + RecordHeaderLayOut.HEADER_SIZE_BYTES
        );

        memoryBlock.copyFromMemoryBlock(
                serviceMemoryBlock,
                tmpRecordAddress,
                recordAddress,
                recordSize
        );

        if ((binaryFunctor != null) && (binaryFunctor.isAssociative())) {
            valueReader.setSource(record);
            assert valueReader.size() == 1;

            calculateNewValue(
                    recordAddress,
                    recordSize,
                    hashCode,
                    binaryKeyValueStorage,
                    binaryComparator,
                    memoryBlock
            );
        } else {
            insertRecord(
                    recordAddress,
                    hashCode,
                    binaryComparator,
                    binaryKeyValueStorage
            );
        }
    }

    protected void onMemoryBlockSwitched(MemoryBlock memoryBlock) {

    }

    private void insertRecord(long recordAddress,
                              long hashCode,
                              BinaryComparator binaryComparator,
                              BinaryKeyValueStorage<T> binaryKeyValueStorage)
            throws IOException {
        long slotAddress = binaryKeyValueStorage.addRecord(
                recordAddress,
                binaryComparator
        );

        if (binaryKeyValueStorage.wasLastSlotCreated()) {
            binaryKeyValueStorage.setKeyHashCode(slotAddress, hashCode);
            onSlotInserted(hashCode);
        }
    }

    protected void onSlotInserted(long hashCode) {
    }

    @Override
    protected void onSpillingStarted() {
        state = AggregatorState.SPILLING;

        getSpiller().start(
                partitions
        );
    }

    @Override
    public boolean spillNextChunk() {
        return state != AggregatorState.SPILLING
                ||
                checkState(getSpiller().processNextChunk(), AggregatorState.AGGREGATING);
    }

    private void calculateNewValue(long recordAddress,
                                   long recordSize,
                                   long hashCode,
                                   BinaryKeyValueStorage binaryKeyValueStorage,
                                   BinaryComparator binaryComparator,
                                   MemoryBlock memoryBlock) throws IOException {
        long slotAddress =
                binaryKeyValueStorage.createOrGetSlot(
                        recordAddress,
                        binaryComparator
                );

        if (binaryKeyValueStorage.wasLastSlotCreated()) {
            onSlotInserted(hashCode);
            binaryKeyValueStorage.setKeyHashCode(slotAddress, hashCode);
        } else {
            calculateRecords(
                    binaryKeyValueStorage,
                    recordAddress,
                    recordSize,
                    slotAddress,
                    memoryBlock
            );
        }
    }

    @SuppressWarnings("unchecked")
    private void calculateRecords(BinaryKeyValueStorage binaryKeyValueStorage,
                                  long recordAddress,
                                  long recordSize,
                                  long slotAddress,
                                  MemoryBlock memoryBlock) {
        long oldRecordAddress =
                binaryKeyValueStorage.getHeaderRecordAddress(slotAddress);
        long oldValueAddress = IOUtil.getValueAddress(
                oldRecordAddress, memoryBlock
        );
        long oldValueSize = IOUtil.getValueWrittenBytes(oldRecordAddress, memoryBlock);
        long valueAddress = IOUtil.getValueAddress(recordAddress, memoryBlock);
        long valueSize = IOUtil.getValueWrittenBytes(recordAddress, memoryBlock);

        binaryFunctor.processStoredData(
                memoryBlock.getAccessor(),
                oldValueAddress,
                oldValueSize,
                valueAddress,
                valueSize,
                useBigEndian
        );

        memoryBlock.free(recordAddress, recordSize + RecordHeaderLayOut.HEADER_SIZE_BYTES);
    }
}
