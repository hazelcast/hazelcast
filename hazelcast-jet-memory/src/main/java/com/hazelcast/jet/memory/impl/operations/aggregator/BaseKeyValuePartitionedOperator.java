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
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.jet.memory.api.memory.OutOfMemoryException;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.api.memory.spilling.spillers.KeyValueStorageSpiller;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueReader;
import com.hazelcast.jet.memory.api.operations.aggregator.AggregationIterator;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.impl.binarystorage.DefaultStorageHeader;
import com.hazelcast.jet.memory.impl.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.impl.memory.impl.management.memorychain.DefaultMemoryBlockChain;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.io.serialization.JetSerializationService;
import com.hazelcast.jet.io.impl.serialization.JetSerializationServiceImpl;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.operations.State;
import com.hazelcast.jet.memory.spi.operations.aggregator.AggregatorState;
import com.hazelcast.jet.memory.spi.operations.aggregator.KeyValueOperator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;

import java.io.File;

public abstract class BaseKeyValuePartitionedOperator<T, P extends KeyValueDataPartition<T>>
        implements KeyValueOperator<T> {
    protected final int partitionCount;
    protected final boolean spillToDisk;
    protected final boolean useBigEndian;
    protected final int partitionCountBase;
    protected final int spillingBufferSize;
    protected final File spillingDirectory;
    protected final JetDataInput jetDataInput;
    protected final JetDataOutput jetDataOutput;
    protected final MemoryContext memoryContext;
    protected final BinaryFunctor binaryFunctor;
    protected final BinaryComparator defaultComparator;
    protected final MemoryChainingType memoryChainingType;
    protected SpillingKeyValueReader spillingKeyValueReader;
    protected final JetSerializationService serializationService
            = new JetSerializationServiceImpl();

    protected final ObjectHolder<BinaryComparator> comparatorHolder
            = new ObjectHolder<BinaryComparator>();

    protected final int spillingChunkSize;
    protected final IOContext ioContext;


    protected P currentPartition;
    protected final P[] partitions;
    protected final StorageHeader header;

    protected final HsaResizeListener hsaResizeListener = new HsaResizeListener() {
        @Override
        public void onHsaResize(long newHsaBaseAddress) {
            header.setBaseStorageAddress(newHsaBaseAddress);
        }
    };

    protected final ContainersPull<T> containersPull;

    protected State state = initialState();
    protected final ElementsWriter<T> keysWriter;
    protected final ElementsWriter<T> valuesWriter;

    protected final MemoryBlock serviceMemoryBlock;
    protected final MemoryBlock temporaryMemoryBlock;


    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public BaseKeyValuePartitionedOperator(int partitionCount,
                                           int spillingBufferSize,
                                           IOContext ioContext,
                                           BinaryComparator binaryComparator,
                                           MemoryContext memoryContext,
                                           MemoryChainingType memoryChainingType,
                                           ElementsWriter<T> keysWriter,
                                           ElementsWriter<T> valuesWriter,
                                           ContainersPull<T> containersPull,
                                           BinaryFunctor binaryFunctor,
                                           String spillingDirectory,
                                           int spillingChunkSize,
                                           boolean spillToDisk,
                                           boolean useBigEndian) {
        this.spillingDirectory = new File(spillingDirectory);
        assertParams(partitionCount, spillToDisk);
        this.keysWriter = keysWriter;
        this.valuesWriter = valuesWriter;
        this.ioContext = ioContext;
        this.spillToDisk = spillToDisk;
        this.useBigEndian = useBigEndian;
        this.binaryFunctor = binaryFunctor;
        this.memoryContext = memoryContext;
        this.partitionCount = partitionCount;
        this.containersPull = containersPull;
        this.memoryChainingType = memoryChainingType;
        this.defaultComparator = binaryComparator;
        this.spillingChunkSize = spillingChunkSize;
        this.partitionCountBase = partitionCount - 1;
        this.spillingBufferSize = spillingBufferSize;
        this.header = new DefaultStorageHeader();
        this.partitions = createPartitions(partitionCount);
        this.jetDataInput = serializationService.createObjectDataInput(null, useBigEndian);
        this.jetDataOutput = serializationService.createObjectDataOutput(null, useBigEndian);
        initPartitions(partitionCount);
        MemoryBlockChain memoryBlockChain =
                new DefaultMemoryBlockChain(
                        memoryContext,
                        false,
                        memoryChainingType
                );
        assert memoryBlockChain.acquireNextBlock(false);
        assert memoryBlockChain.acquireNextBlock(false);
        this.serviceMemoryBlock = memoryBlockChain.getElement(0);
        this.temporaryMemoryBlock = memoryBlockChain.getElement(1);
    }

    protected abstract P[] createPartitions(int partitionCount);

    private void assertParams(int partitionCount,
                              boolean spillToDisk) {
        assert Util.isPositivePowerOfTwo(partitionCount)
                :
                "Partition count should be power of two";

        assert !spillToDisk
                ||
                (this.spillingDirectory.exists()
                        &&
                        this.spillingDirectory.isDirectory()
                        &&
                        this.spillingDirectory.canWrite())
                :
                "Invalid directory for spilling";
    }

    private void initPartitions(int partitionCount) {
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = createPartition(i);
        }
    }

    protected State initialState() {
        return AggregatorState.AGGREGATING;
    }

    protected abstract P createPartition(int partitionID);

    protected void onHeaderAllocated(MemoryBlock memoryBlock) {
    }

    @Override
    public void startSpilling() {
        if (!spillToDisk) {
            dispose();
            throw new OutOfMemoryException("Not enough memory to aggregate");
        }

        onSpillingStarted();
    }

    protected abstract void onSpillingStarted();

    protected void onSpillingFinished() {
        getSpiller().stop();
        resetBlocks();
        activatePartitions();
    }

    protected abstract KeyValueStorageSpiller<T, P> getSpiller();

    private void resetBlocks() {
        for (P partition : partitions) {
            for (int i = 0; i < partition.getMemoryBlockChain().size(); i++) {
                MemoryBlock memoryBlock = partition.getMemoryBlockChain().getElement(i);
                memoryBlock.reset(true);
                onBlockReset(memoryBlock);
                header.setMemoryBlock(memoryBlock);
                header.resetHeader();
            }

            partition.getMemoryBlockChain().gotoElement(0);
        }
    }

    protected void onBlockReset(MemoryBlock memoryBlock) {

    }

    protected abstract AggregationIterator<T> createResultIterator();

    @Override
    public void finishSpilling() {
        onSpillingFinished();
        state = AggregatorState.AGGREGATING;
    }

    protected boolean checkState(boolean result,
                                 State nextState) {
        if (result) {
            state = nextState;
        }

        return result;
    }

    protected BinaryComparator getComparator() {
        return comparatorHolder.getObject(defaultComparator);
    }

    @Override
    public void setComparator(BinaryComparator comparator) {
        comparatorHolder.setObject(comparator);
    }

    @Override
    public void dispose() {
        try {
            for (P partition : partitions) {
                partition.getMemoryBlockChain().dispose();
            }
        } finally {
            disposeSpiller();
        }
    }

    protected void activatePartitions() {
        for (KeyValueDataPartition partition : partitions) {
            MemoryBlock memoryBlock =
                    partition.getMemoryBlockChain().activeElement();
            activateMemoryBlock(partition, memoryBlock);
        }
    }

    protected void activateMemoryBlock(KeyValueDataPartition partition,
                                       MemoryBlock memoryBlock) {
        memoryBlock.reset(true);
        partition.getKeyValueStorage().setMemoryBlock(
                memoryBlock
        );
        header.setMemoryBlock(memoryBlock);
        header.allocatedHeader();
        onHeaderAllocated(memoryBlock);
        long baseAddress = partition.getKeyValueStorage().gotoNew();
        header.setBaseStorageAddress(baseAddress);
    }

    private void disposeSpiller() {
        try {
            getSpiller().dispose();
        } finally {
            Util.deleteFile(spillingDirectory);
        }
    }
}
