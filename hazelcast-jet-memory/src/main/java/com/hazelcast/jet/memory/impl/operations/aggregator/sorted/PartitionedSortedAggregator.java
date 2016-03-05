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

package com.hazelcast.jet.memory.impl.operations.aggregator.sorted;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.operations.BinaryDataFetcher;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.impl.binarystorage.BinaryStorageFactory;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.DataSorter;
import com.hazelcast.jet.memory.api.operations.aggregator.AggregationIterator;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.InputsIterator;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueWriter;
import com.hazelcast.jet.memory.spi.operations.aggregator.sorting.SortedAggregator;
import com.hazelcast.jet.memory.api.memory.spilling.spillers.KeyValueStorageSpiller;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueSortedDataPartition;
import com.hazelcast.jet.memory.impl.operations.aggregator.sorted.sorters.SpillingHeapSorter;
import com.hazelcast.jet.memory.impl.operations.aggregator.BaseKeyValuePartitionedAggregator;
import com.hazelcast.jet.memory.impl.operations.partition.DefaultKeyValueSortedDataPartition;
import com.hazelcast.jet.memory.impl.memory.impl.management.memorychain.DefaultMemoryBlockChain;
import com.hazelcast.jet.memory.impl.operations.aggregator.sorted.iterators.DefaultInputsIterator;
import com.hazelcast.jet.memory.impl.operations.aggregator.sorted.sorters.IteratingHeapSorter;
import com.hazelcast.jet.memory.impl.operations.aggregator.iterator.sorted.SortedAggregationIterator;
import com.hazelcast.jet.memory.impl.memory.impl.spilling.spillers.sorted.KeyValueSortedStorageSpillerImpl;

import java.util.Iterator;

/**
 * This is abstract binary aggregator representation;
 * <p>
 * Data are stored in the following format;
 * <p>
 * <pre>
 * -------------------------------------------------------------------------
 * | MemoryBlock1 |   MemoryBlock2  | .........                |  BlockN   |
 * |              |                 |                          |           |
 * |    First     |   Next          |                          |   Last    |
 * -------------------------------------------------------------------------
 * </pre>
 * <p>
 * Logically data represented as:
 * <p>
 * <p>
 * <pre>
 *     Partitions:
 * ----------------------------------------------------------------------------------
 * |   Partition1   |                                           |    PartitionN     |
 * ----------------------------------------------------------------------------------
 * |   Key-Value    |                                           |     Key-Value     |
 * |    storage1    |                                           |     storageN      |
 * ----------------------------------------------------------------------------------
 * <pre/>
 *
 * Each Memory block has a header:
 * <pre>
 * ----------------------------------------------------
 * | partition_id_1(4 bytes)|storage_slot_1 (16 bytes)|
 * ----------------------------------------------------
 * |      ......................................      |
 * ----------------------------------------------------
 * |partition_id_N (4 bytes)|storage_slot_N (16 bytes)|
 * * --------------------------------------------------
 * </pre>
 * <p>
 * Each storage slot has the following structure:
 * <pre>
 *
 * ---------------------------------------------------------------------
 * | Key-value storage base address (8 bytes)                          |
 * ---------------------------------------------------------------------
 * |Reference to the nextKey key-value storage base address (8 bytes)  |
 * ---------------------------------------------------------------------
 *
 * </pre>
 * Inside each block data are stored as key value storage;
 * <p>
 * When aggregator switches to the nextSlot memory block - it creates new version of key-value storage
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
 * After that nextSlot Memory block is acquired from the pool and data are serialized and put
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
 * If BinaryFunctor is presented but is not associative, the result of the binaryFunctor will be calculated finally on
 * iteration phase;
 * <p>
 * <p>
 * This aggregator supports sorting.
 * <p>
 * During each put - if not more block to put there are following steps performed:
 * <p>
 * For the last block:
 * <p>
 *  1) Partition one by one quick sorting - if it is necessary (OpenAddressing storage)
 *  2) Iterable partitions merge sorting to another block using the following scheme:
 * <p>
 * <pre>
 *     MergeSort(SortedPartitions1, SortedPartition2) -> ServiceBlock with spilled data format
 *     MergeSort(SortedPartition3,  ServiceBlock with spilled data format) -> ServiceBlock with spilled data format
 *      ...................................
 * </pre>
 * <p>
 * Finally we have sorted and spilled data as one memory block;
 * Using merge sort with previously spilled and sorted data on disk - spill and sort
 * memory block and disk data as new disk block (spilled and sorted).
 * <p>
 * <p>
 * <p>
 * Finally after all puts iteration has a following way:
 * <p>
 *  If data was spilled on disk:
 *          1) For each block despite last:
 *              -   Partition one by one quick sorting
 *              -   Partitions merge sorting with last spilled block and spilling to another block
 *              (as described above)
 *          2) Last block:
 *              - Partition one by one quick sorting
 *              - Partitions merge sorting to another block
 *              - Block and spilled merge sorting -> to disk a merged data
 * <p>
 *          Iterate as merge sort for spilled and sorted to memory and spilled and sorted to disk.
 * <p>
 * If doesn't have spilled:
 *          1) For each block:
 *              -   Partition one by one quick sorting
 *              -   Partitions merge sorting with last spilled block and spilling to another block
 * <p>
 *          Iterate over spilled and merged to memory blocks
 *
 * @param <T>-type of the input container of key-value pairs;
 */
public class PartitionedSortedAggregator<T>
        extends BaseKeyValuePartitionedAggregator<T, KeyValueSortedDataPartition<T>>
        implements SortedAggregator<T> {
    private final InputsIterator inputsIterator;

    private final AggregationIterator<T> iterator;

    private final DataSorter<InputsIterator, SpillingKeyValueWriter> spillingSorter;

    private final KeyValueStorageSpiller<T, KeyValueSortedDataPartition<T>> spiller;

    private final MemoryBlockChain sortedMemoryBlockChain = new DefaultMemoryBlockChain();

    private final DataSorter<InputsIterator, BinaryDataFetcher<T>> memoryDiskMergeSorter;

    private final DataSorter<KeyValueSortedDataPartition[], MemoryBlockChain> memoryBlocksSorter;

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public PartitionedSortedAggregator(int partitionCount,
                                       int spillingBufferSize,
                                       IOContext ioContext,
                                       BinaryComparator binaryComparator,
                                       MemoryContext memoryContext,
                                       MemoryChainingType memoryChainingType,
                                       ElementsWriter<T> keyWriter,
                                       ElementsWriter<T> valueWriter,
                                       ContainersPull<T> containersPull,
                                       String spillingDirectory,
                                       OrderingDirection orderingDirection,
                                       int spillingChunkSize,
                                       boolean spillToDisk,
                                       boolean useBigEndian) {
        this(
                partitionCount,
                spillingBufferSize,
                ioContext,
                binaryComparator,
                memoryContext,
                memoryChainingType,
                keyWriter,
                valueWriter,
                containersPull,
                null,
                spillingDirectory,
                orderingDirection,
                spillingChunkSize,
                spillToDisk,
                useBigEndian
        );
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public PartitionedSortedAggregator(int partitionCount,
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
                                       OrderingDirection orderingDirection,
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

        this.memoryBlocksSorter = new DefaultMemoryBlocksSorter(
                header,
                orderingDirection
        );

        this.memoryDiskMergeSorter = new IteratingHeapSorter<T>(
                temporaryMemoryBlock,
                keysWriter,
                valuesWriter,
                orderingDirection,
                binaryComparator,
                comparatorHolder,
                binaryFunctor,
                useBigEndian
        );

        this.inputsIterator = new DefaultInputsIterator(
                orderingDirection,
                BinaryStorageFactory.getSortedStorage(
                        binaryComparator,
                        hsaResizeListener
                ),
                serviceMemoryBlock
        );


        this.spillingSorter = new SpillingHeapSorter(
                spillingChunkSize,
                orderingDirection,
                comparatorHolder,
                binaryComparator,
                binaryFunctor,
                temporaryMemoryBlock,
                useBigEndian
        );

        this.spiller = createSpiller();
        this.iterator = createResultIterator();
        activatePartitions();
    }

    private KeyValueStorageSpiller<T, KeyValueSortedDataPartition<T>> createSpiller() {
        return new KeyValueSortedStorageSpillerImpl<T>(
                spillingBufferSize,
                spillingDirectory,
                sortedMemoryBlockChain,
                memoryBlocksSorter,
                spillingSorter,
                inputsIterator,
                useBigEndian
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    protected KeyValueSortedDataPartition<T>[] createPartitions(int partitionCount) {
        return new KeyValueSortedDataPartition[partitionCount];
    }

    @Override
    protected KeyValueSortedDataPartition<T> createPartition(int partitionId) {
        return new DefaultKeyValueSortedDataPartition<T>(
                partitionId,
                memoryContext,
                memoryChainingType,
                defaultComparator,
                hsaResizeListener
        );
    }

    @Override
    protected KeyValueStorageSpiller<T, KeyValueSortedDataPartition<T>> getSpiller() {
        return spiller;
    }

    @Override
    public Iterator<T> iterator() {
        inputsIterator.setInputs(
                getSpiller().openSpillingReader(),
                sortedMemoryBlockChain
        );

        iterator.reset(getComparator());
        return iterator;
    }

    @Override
    protected AggregationIterator<T> createResultIterator() {
        return new SortedAggregationIterator<T>(
                serviceMemoryBlock,
                temporaryMemoryBlock,
                memoryDiskMergeSorter,
                binaryFunctor,
                containersPull,
                partitions,
                header,
                keysWriter,
                valuesWriter,
                ioContext,
                inputsIterator,
                useBigEndian
        );
    }

    @Override
    public void startSorting() {
        memoryBlocksSorter.open(partitions, sortedMemoryBlockChain);
    }

    @Override
    public boolean sort() {
        return memoryBlocksSorter.sort();
    }

    @Override
    public void dispose() {
        try {
            super.dispose();
        } finally {
            sortedMemoryBlockChain.clear();
        }
    }
}
