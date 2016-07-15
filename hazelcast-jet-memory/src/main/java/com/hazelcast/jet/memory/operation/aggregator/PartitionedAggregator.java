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
import com.hazelcast.jet.memory.binarystorage.HashStorage;
import com.hazelcast.jet.memory.binarystorage.Storage;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.operation.aggregator.cursor.InMemoryCursor;
import com.hazelcast.jet.memory.operation.aggregator.cursor.SpillingCursor;
import com.hazelcast.jet.memory.operation.aggregator.cursor.TupleCursor;
import com.hazelcast.jet.memory.spilling.DefaultSpiller;
import com.hazelcast.jet.memory.spilling.Spiller;


/**
 * Data are stored in the following format:
 * <pre>
 *     Partition1:
 * --------------------------------------...-------------------------
 * | MemoryBlock1 |    MemoryBlock2     |...|      |  MemoryBlockN   |
 * |              |                     |...|      |                 |
 * |    First     |    Next             |...|      |  Last           |
 * --------------------------------------...--------------------------
 *    Partition2:
 * --------------------------------------...--------------------------
 * | MemoryBlock1 |    MemoryBlock2     |...|      |  MemoryBlockN   |
 * |              |                     |...|      |                 |
 * |    First     |    Next             |...|      |  Last           |
 * --------------------------------------...--------------------------
 *    .......
 *    .......                            ...
 *                                       ...
 *    PartitionN:
 * --------------------------------------...--------------------------
 * | MemoryBlock1 |    MemoryBlock2     |...|      |  MemoryBlockN   |
 * |              |                     |...|      |                 |
 * |    First     |    Next             |...|      |  Last           |
 * --------------------------------------...--------------------------
 * </pre>
 * <p>
 * Each storage slot has the following structure:
 * <pre>
 * --------------------------------------------
 * | Key-value storage base address (8 bytes) |
 * --------------------------------------------
 * </pre>
 * Inside each block data are stored in a key-value structure.
 * <p>
 * When the aggregator switches to the next memory block, it creates a new version of
 * the key-value structure for each partition.
 * <p>
 * The algorithm of aggregation is as follows:
 * <ol><li>
 * Data represented inside input object of type {@code } is extracted as key-part
 * and value-part using the corresponding {@code ElementsReader} objects.
 * </li><li>
 * The next Memory block is acquired from the pool, the data is serialized and put into the
 * data structure as key-value pair.
 * </li><li>
 * If no more memory blocks are available, data is spilled over to disk and the memory block is reused.
 * </li><li>
 * When reloading the spilled data, the data from the last memory block is merged with the data read from disk.
 * </li><li>
 * In the end the user can iterate over the result.
 * </li></ol>
 * <p>
 * Using an associative accumulator it is possible to accumulate the result for a certain key, for example:
 * <pre>
 *     data[key] = data[key] + nextCount;
 * </pre>
 * If the accumulator is not associative, the result of accumulation will be calculated at the end,
 * in the iteration phase.
 * <p>
 * If there is no accumulator, the values will be written as a list under the corresponding key.
 * <p>
 * No sorting is performed for this type of aggregation.
 */
public class PartitionedAggregator extends PartitionedAggregatorBase {
    protected final TupleCursor cursor;
    protected final Spiller spiller;
    protected final Storage serviceKeyValueStorage;

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public PartitionedAggregator(
            int partitionCount, int spillingBufferSize, IOContext ioContext, Comparator comparator,
            MemoryContext memoryContext, MemoryChainingRule memoryChainingRule, Tuple2 destTuple,
            String spillingDirectory, int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        this(partitionCount, spillingBufferSize, ioContext, comparator, memoryContext,
                memoryChainingRule, destTuple, null, spillingDirectory, spillingChunkSize,
                spillToDisk, useBigEndian
        );
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public PartitionedAggregator(
            int partitionCount, int spillingBufferSize, IOContext ioContext, Comparator comparator,
            MemoryContext memoryContext, MemoryChainingRule memoryChainingRule, Tuple2 destTuple,
            Accumulator accumulator, String spillingDirectory,
            int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        super(partitionCount, spillingBufferSize, ioContext, comparator, memoryContext, memoryChainingRule,
                destTuple, accumulator, spillingDirectory, spillingChunkSize, spillToDisk, useBigEndian);
        this.serviceKeyValueStorage = new HashStorage(null, comparator.getHasher(), hsaResizeListener);
        this.spiller = newSpiller();
        this.cursor = newResultCursor();
        activatePartitions();
    }

    @Override
    protected Spiller spiller() {
        return spiller;
    }

    protected Spiller newSpiller() {
        return new DefaultSpiller(serviceMemoryBlock, accumulator, spillingBufferSize, spillingChunkSize,
                spillingDirectory, useBigEndian);
    }

    @Override
    protected Partition newPartition(int partitionID) {
        return Partition.newPartition(partitionID, memoryContext, memoryChainingRule, defaultComparator, hsaResizeListener);
    }

    @Override
    protected TupleCursor newResultCursor() {
        return new PartitionedTupleCursor();
    }

    @Override
    public TupleCursor cursor() {
        cursor.reset(getComparator());
        spillFileCursor = spiller.openSpillFileCursor();
        return cursor;
    }

    private class PartitionedTupleCursor implements TupleCursor {
        private final TupleCursor memoryCursor;
        private final TupleCursor spillingCursor;
        private boolean spillingCursorDone;

        public PartitionedTupleCursor() {
            this.spillingCursor = new SpillingCursor(
                    serviceMemoryBlock, temporaryMemoryBlock, accumulator, spiller, destTuple, partitions,
                    header, ioContext, useBigEndian);
            this.memoryCursor = new InMemoryCursor(
                    serviceKeyValueStorage, serviceMemoryBlock, temporaryMemoryBlock, accumulator, destTuple,
                    partitions, header, ioContext, useBigEndian);
        }

        @Override
        public void reset(Comparator comparator) {
            spillingCursorDone = false;
            spillingCursor.reset(comparator);
            memoryCursor.reset(comparator);
        }

        @Override
        public boolean advance() {
            if (spillingCursorDone) {
                return memoryCursor.advance();
            }
            if (spillingCursor.advance()) {
                return true;
            }
            spillingCursorDone = true;
            return memoryCursor.advance();
        }

        @Override
        public Tuple2 asTuple() {
            return (spillingCursorDone ? memoryCursor : spillingCursor).asTuple();
        }
    }
}
