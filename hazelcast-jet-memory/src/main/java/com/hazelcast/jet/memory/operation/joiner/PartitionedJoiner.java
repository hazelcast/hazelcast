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

package com.hazelcast.jet.memory.operation.joiner;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.operation.aggregator.JoinAggregator;
import com.hazelcast.jet.memory.operation.aggregator.PartitionedAggregator;
import com.hazelcast.jet.memory.operation.aggregator.cursor.InMemoryCursor;
import com.hazelcast.jet.memory.operation.aggregator.cursor.SpillingCursor;
import com.hazelcast.jet.memory.operation.aggregator.cursor.TupleCursor;
import com.hazelcast.jet.memory.spilling.DefaultSpiller;

/**
 * This is abstract binary joiner representation;
 * This implementation is based on binary aggregation;
 * <p>
 * <p>
 * Data are stored in the following format;
 * <p>
 * <pre>
 *     Partition1:
 *
 * Source1:
 * ---------------------------------------------------------------
 * | MemoryBlock1_1 |    MemoryBlock1_2   |...|      |  Block1_N |
 * |                |                     |   |      |           |
 * |    First       |    Next             |   |      |  Last     |
 * ---------------------------------------------------------------
 *
 * Source2:
 * ---------------------------------------------------------------
 * | MemoryBlock2_1 |    MemoryBlock2_2   |...|      |  Block2_N |
 * |                |                     |   |      |           |
 * |    First       |    Next             |   |      |  Last     |
 * ---------------------------------------------------------------
 *    Partition2:
 *
 * Source1:
 * ---------------------------------------------------------------
 * | MemoryBlock1_1 |    MemoryBlock1_2   |...|      |  Block1_N |
 * |                |                     |   |      |           |
 * |    First       |    Next             |   |      |  Last     |
 * ---------------------------------------------------------------
 *
 * Source2:
 * ---------------------------------------------------------------
 * | MemoryBlock2_1 |    MemoryBlock2_2   |...|      |  Block2_N |
 * |                |                     |   |      |           |
 * |    First       |    Next             |   |      |  Last     |
 * ---------------------------------------------------------------
 * ....................
 *
 * ....................
 * Source1:
 * ---------------------------------------------------------------
 * | MemoryBlock1_1 |    MemoryBlock1_2   |...|      |  Block1_N |
 * |                |                     |   |      |           |
 * |    First       |    Next             |   |      |  Last     |
 * ---------------------------------------------------------------
 *
 * Source2:
 * ---------------------------------------------------------------
 * | MemoryBlock2_1 |    MemoryBlock2_2   |...|      |  Block2_N |
 * |                |                     |   |      |           |
 * |    First       |    Next             |   |      |  Last     |
 * ---------------------------------------------------------------
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
 * The algorithm of aggregation is following:
 * <p>
 * Data represented inside input object of type  are extracted as key-part
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
 */
public class PartitionedJoiner extends PartitionedAggregator implements JoinAggregator {

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public PartitionedJoiner(
            int partitionCount, int spillingBufferSize, IOContext ioContext, Comparator comparator,
            MemoryContext memoryContext, MemoryChainingRule memoryChainingRule, Tuple2 tuple, String spillingDirectory,
            int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        super(partitionCount, spillingBufferSize, ioContext, comparator, memoryContext,
                memoryChainingRule, tuple, spillingDirectory, spillingChunkSize, spillToDisk,
                useBigEndian);
    }


    @Override
    public TupleCursor cursor() {
        cursor.reset(getComparator());
        spillFileCursor = this.spiller.openSpillFileCursor();
        return cursor;
    }

    @Override
    protected DefaultSpiller newSpiller() {
        return new DefaultSpiller(serviceMemoryBlock, accumulator, spillingBufferSize, spillingChunkSize,
                spillingDirectory, useBigEndian);
    }

    @Override
    protected TupleCursor newResultCursor() {
        return new DefaultJoinerCursor();
    }

    @Override
    public void setSource(short source) {
        assert source == 0;
    }

    private class DefaultJoinerCursor implements TupleCursor {
        private final TupleCursor memoryCursor;
        private final TupleCursor spillingCursor;
        private boolean spillingCursorDone;

        public DefaultJoinerCursor() {
            this.spillingCursor = new SpillingCursor(serviceMemoryBlock, temporaryMemoryBlock, accumulator,
                    spiller, destTuple, partitions, header, ioContext, useBigEndian);
            this.memoryCursor = new InMemoryCursor(serviceKeyValueStorage, serviceMemoryBlock,
                    temporaryMemoryBlock, accumulator, destTuple, partitions, header, ioContext, useBigEndian);
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
