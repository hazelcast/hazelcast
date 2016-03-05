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
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.api.binarystorage.BloomFilter;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.impl.binarystorage.DefaultBloomFilter;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.api.operations.aggregator.AggregationIterator;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.api.memory.spilling.spillers.KeyValueStorageSpiller;
import com.hazelcast.jet.memory.impl.operations.aggregator.iterator.InMemoryIterator;
import com.hazelcast.jet.memory.impl.operations.aggregator.iterator.SpillingIterator;
import com.hazelcast.jet.memory.impl.binarystorage.BinaryKeyValueOpenAddressingStorage;
import com.hazelcast.jet.memory.impl.operations.partition.DefaultKeyValueDataPartition;
import com.hazelcast.jet.memory.impl.memory.impl.spilling.spillers.KeyValueStorageSpillerImpl;

import java.util.Iterator;


/**
 * This is abstract binary aggregator representation;
 * <p>
 * Data are stored in the following format;
 * <p>
 * <pre>
 *     Partition1:
 * -------------------------------------------------------------
 * | MemoryBlock1 |    MemoryBlock2     |...|      |  BlockN   |
 * |              |                     |   |      |           |
 * |    First     |    Next             |   |      |  Last     |
 * -------------------------------------------------------------
 *    Partition2:
 * -------------------------------------------------------------
 * | MemoryBlock1 |    MemoryBlock2     |...|      |  BlockN   |
 * |              |                     |   |      |           |
 * |    First     |    Next             |   |      |  Last     |
 * -------------------------------------------------------------
 * ....................
 *
 * ....................
 *    PartitionN:
 * -------------------------------------------------------------
 * | MemoryBlock1 |    MemoryBlock2     |...|      |  BlockN   |
 * |              |                     |   |      |           |
 * |    First     |    Next             |   |      |  Last     |
 * -------------------------------------------------------------
 * </pre>
 * <p>
 * <pre/>
 * Each storage slot has the following structure:
 * <pre>
 * --------------------------------------------
 * | Key-value storage base address (8 bytes) |
 * --------------------------------------------
 * </pre>
 * Inside each block data are stored as key value storage;
 * <p>
 * When aggregator switches to the next memory block - it creates new version of key-value storage
 * for each partition;
 * <p>
 * The type of the storage is being specified as storageType param in constructor;
 * <p>
 * It is either: Red-Black tree storage or OpenAddressing storage;
 * <p>
 * The algorithm of aggregation is following:
 * <p>
 * Data represented inside input object of type <T> are extracted as key-part
 * and value part using corresponding ElementsReader objects;
 * <p>
 * After that next Memory block is acquired from the pool and data are serialized and put
 * to the corresponding binary storage as key-value pair;
 * <p>
 * If no more active blocks available data are spilled on the disk and block is re-used;
 * <p>
 * While spilling data from the last memory-block and disk are being merged;
 * <p>
 * Once all data written, user can iterate over result;
 * <p>
 * Using associative BinaryFunctor it is possible to collect last value for the certain key, for example:
 * <p>
 * <pre>
 *     data[key] = data[key] + nextCount;
 * </pre>
 * <p>
 * Without associative BinaryFunctor values will be written as list under the corresponding key;
 * <p>
 * If BinaryFunctor is presented but is not associative , the result of the binaryFunctor will be calculated finally on
 * iteration phase;
 * <p>
 * <p>
 * No sorting performed for this type of aggregation;
 *
 * @param <T> - type of the input container of key-value pairs;
 */
public class PartitionedAggregator<T>
        extends BaseKeyValuePartitionedAggregator<T, KeyValueDataPartition<T>> {
    protected final BloomFilter bloomFilter;
    protected final AggregationIterator<T> iterator;
    protected final KeyValueStorageSpiller<T, KeyValueDataPartition<T>> spiller;
    protected final BinaryKeyValueStorage<T> serviceKeyValueStorage;

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public PartitionedAggregator(int partitionCount,
                                 int spillingBufferSize,
                                 int bloomFilterSizeInBytes,
                                 IOContext ioContext,
                                 BinaryComparator binaryComparator,
                                 MemoryContext memoryContext,
                                 MemoryChainingType memoryChainingType,
                                 ElementsWriter<T> keyWriter,
                                 ElementsWriter<T> valueWriter,
                                 ContainersPull<T> containersPull,
                                 String spillingDirectory,
                                 int spillingChunkSize,
                                 boolean spillToDisk,
                                 boolean useBigEndian) {
        this(
                partitionCount,
                spillingBufferSize,
                bloomFilterSizeInBytes,
                ioContext,
                binaryComparator,
                memoryContext,
                memoryChainingType,
                keyWriter,
                valueWriter,
                containersPull,
                null,
                spillingDirectory,
                spillingChunkSize,
                spillToDisk,
                useBigEndian
        );
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public PartitionedAggregator(int partitionCount,
                                 int spillingBufferSize,
                                 int bloomFilterSizeInBytes,
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

        this.bloomFilter = new DefaultBloomFilter(
                bloomFilterSizeInBytes,
                this.header.getSize() + MemoryBlock.TOP_OFFSET
        );

        this.serviceKeyValueStorage = new BinaryKeyValueOpenAddressingStorage<T>(
                null,
                binaryComparator.getHasher(),
                hsaResizeListener
        );

        this.spiller = createSpiller();
        this.iterator = createResultIterator();
        activatePartitions();
    }

    @Override
    protected void onMemoryBlockSwitched(MemoryBlock memoryBlock) {
        bloomFilter.setMemoryBlock(memoryBlock);
    }

    @Override
    protected void onSlotInserted(long hashCode) {
        bloomFilter.mark(hashCode);
    }

    @Override
    protected void onHeaderAllocated(MemoryBlock memoryBlock) {
        bloomFilter.setMemoryBlock(memoryBlock);
        bloomFilter.allocateBloomFilter();
    }

    @Override
    protected KeyValueStorageSpiller<T, KeyValueDataPartition<T>> getSpiller() {
        return spiller;
    }

    @Override
    protected void onBlockReset(MemoryBlock memoryBlock) {
        bloomFilter.setMemoryBlock(memoryBlock);
        bloomFilter.reset();
    }

    protected KeyValueStorageSpiller<T, KeyValueDataPartition<T>> createSpiller() {
        return new KeyValueStorageSpillerImpl<T>(
                bloomFilter,
                serviceMemoryBlock,
                binaryFunctor,
                spillingBufferSize,
                spillingChunkSize,
                spillingDirectory,
                useBigEndian
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    protected KeyValueDataPartition<T>[] createPartitions(int partitionCount) {
        return new KeyValueDataPartition[partitionCount];
    }

    @Override
    protected KeyValueDataPartition<T> createPartition(int partitionID) {
        return new DefaultKeyValueDataPartition<T>(
                partitionID,
                memoryContext,
                memoryChainingType,
                defaultComparator,
                hsaResizeListener
        );
    }

    @Override
    protected AggregationIterator<T> createResultIterator() {
        return new DefaultAggregatorIterator();
    }

    @Override
    public Iterator<T> iterator() {
        iterator.reset(getComparator());
        spillingKeyValueReader = this.spiller.openSpillingReader();
        return iterator;
    }

    private class DefaultAggregatorIterator implements AggregationIterator<T> {
        private boolean spilledDone;
        private final AggregationIterator<T> memoryIterator;
        private final AggregationIterator<T> spillingIterator;

        public DefaultAggregatorIterator() {
            assert bloomFilter != null;

            this.spillingIterator =
                    new SpillingIterator<T, KeyValueDataPartition<T>>(
                            serviceMemoryBlock,
                            temporaryMemoryBlock,
                            binaryFunctor,
                            spiller,
                            containersPull,
                            partitions,
                            header,
                            keysWriter,
                            valuesWriter,
                            bloomFilter,
                            ioContext,
                            useBigEndian
                    );
            this.memoryIterator =
                    new InMemoryIterator<T>(
                            serviceKeyValueStorage,
                            serviceMemoryBlock,
                            temporaryMemoryBlock,
                            binaryFunctor,
                            containersPull,
                            partitions,
                            header,
                            keysWriter,
                            valuesWriter,
                            bloomFilter,
                            ioContext,
                            useBigEndian
                    );
        }

        /**
         * Used as storage for the temporary data from disk;
         */
        @Override
        public boolean hasNext() {
            if (!spilledDone) {
                if (spillingIterator.hasNext()) {
                    return true;
                }
                spilledDone = true;
            }

            return memoryIterator.hasNext();
        }

        @Override
        public T next() {
            if (
                    (spilledDone)
                            && (memoryIterator.hasNext())
                    ) {
                return memoryIterator.next();
            }

            if (
                    (!spilledDone) &&
                            (spillingIterator.hasNext())) {
                return spillingIterator.next();
            }

            throw new IllegalStateException("Iterator doesn't have next element");
        }

        @Override
        public void reset(BinaryComparator comparator) {
            spilledDone = false;
            spillingIterator.reset(comparator);
            memoryIterator.reset(comparator);
        }
    }
}
