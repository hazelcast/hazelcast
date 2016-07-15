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

package com.hazelcast.jet.memory.operation.aggregator;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.TupleFetcher;
import com.hazelcast.jet.memory.binarystorage.SortOrder;
import com.hazelcast.jet.memory.binarystorage.SortedHashStorage;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.DefaultMemoryBlockChain;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.operation.aggregator.cursor.InputsCursor;
import com.hazelcast.jet.memory.operation.aggregator.cursor.SortedTupleCursor;
import com.hazelcast.jet.memory.operation.aggregator.cursor.TupleCursor;
import com.hazelcast.jet.memory.operation.aggregator.sorter.IteratingHeapSorter;
import com.hazelcast.jet.memory.operation.aggregator.sorter.MemoryBlockSorter;
import com.hazelcast.jet.memory.operation.aggregator.sorter.Sorter;
import com.hazelcast.jet.memory.operation.aggregator.sorter.SpillingHeapSorter;
import com.hazelcast.jet.memory.spilling.SortedSpiller;
import com.hazelcast.jet.memory.spilling.Spiller;
import com.hazelcast.jet.memory.spilling.SpillingKeyValueWriter;

import static com.hazelcast.jet.memory.Partition.newSortedPartition;

/**
 * Data are stored in the following format;
 * <pre>
 * -------------------------------------------------------------------------
 * | MemoryBlock1 |   MemoryBlock2  | .........                |  BlockN   |
 * |              |                 |                          |           |
 * |    First     |   Next          |                          |   Last    |
 * -------------------------------------------------------------------------
 * </pre>
 * Logically data represented as:
 * <pre>
 *     Partitions:
 * ----------------------------------------------------------------------------------
 * |   Partition1   |                                           |    PartitionN     |
 * ----------------------------------------------------------------------------------
 * |   Key-Value    |                                           |     Key-Value     |
 * |    storage1    |                                           |     storageN      |
 * ----------------------------------------------------------------------------------
 * </pre>
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
 * Data represented inside input object of type  are extracted as key-part
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
 *          1) For each block except last:
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
 */
public class SortedPartitionedAggregator
extends PartitionedAggregatorBase implements SortedAggregator {
    private final InputsCursor inputsCursor;
    private final TupleCursor cursor;
    private final Sorter<InputsCursor, SpillingKeyValueWriter> spillingSorter;
    private final Spiller spiller;
    private final MemoryBlockChain sortedMemoryBlockChain = new DefaultMemoryBlockChain();
    private final Sorter<InputsCursor, TupleFetcher> memoryDiskMergeSorter;
    private final Sorter<Partition[], MemoryBlockChain> memoryBlocksSorter;

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public SortedPartitionedAggregator(
            int partitionCount, int spillingBufferSize, IOContext ioContext, Comparator comparator,
            MemoryContext memoryContext, MemoryChainingRule memoryChainingRule,
            Tuple2 destTuple, String spillingDirectory, SortOrder sortOrder,
            int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        this(partitionCount, spillingBufferSize, ioContext, comparator, memoryContext, memoryChainingRule,
                destTuple, null, spillingDirectory, sortOrder, spillingChunkSize, spillToDisk, useBigEndian);
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public SortedPartitionedAggregator(
            int partitionCount, int spillingBufferSize, IOContext ioContext, Comparator comparator,
            MemoryContext memoryContext, MemoryChainingRule memoryChainingRule, Tuple2 destTuple,
            Accumulator accumulator, String spillingDirectory, SortOrder sortOrder,
            int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        super(partitionCount, spillingBufferSize, ioContext, comparator, memoryContext, memoryChainingRule,
                destTuple, accumulator, spillingDirectory, spillingChunkSize, spillToDisk, useBigEndian);
        this.memoryBlocksSorter = new MemoryBlockSorter(header, sortOrder);
        this.memoryDiskMergeSorter = new IteratingHeapSorter(
                temporaryMemoryBlock, sortOrder, comparator, comparatorHolder, accumulator, useBigEndian);
        this.inputsCursor = new InputsCursor(
                sortOrder, new SortedHashStorage(null, comparator, hsaResizeListener), serviceMemoryBlock);
        this.spillingSorter = new SpillingHeapSorter(spillingChunkSize, sortOrder, comparatorHolder, comparator,
                accumulator, temporaryMemoryBlock, useBigEndian);
        this.spiller = createSpiller();
        this.cursor = newResultCursor();
        activatePartitions();
    }

    @Override
    protected Partition newPartition(int partitionId) {
        return newSortedPartition(partitionId, memoryContext, memoryChainingRule, defaultComparator, hsaResizeListener);
    }

    @Override
    protected Spiller spiller() {
        return spiller;
    }

    @Override
    public TupleCursor cursor() {
        inputsCursor.setInputs(spiller().openSpillFileCursor(), sortedMemoryBlockChain);
        cursor.reset(getComparator());
        return cursor;
    }

    @Override
    protected TupleCursor newResultCursor() {
        return new SortedTupleCursor(serviceMemoryBlock, temporaryMemoryBlock, memoryDiskMergeSorter,
                accumulator, destTuple, partitions, header, ioContext, inputsCursor,
                useBigEndian);
    }

    @Override
    public void prepareToSort() {
        memoryBlocksSorter.resetTo(partitions, sortedMemoryBlockChain);
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

    private Spiller createSpiller() {
        return new SortedSpiller(spillingBufferSize, spillingDirectory, sortedMemoryBlockChain, memoryBlocksSorter,
                spillingSorter, inputsCursor, useBigEndian);
    }
}
