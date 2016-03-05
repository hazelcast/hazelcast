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

package com.hazelcast.jet.memory.impl.operations.joiner;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.operations.aggregator.JoinAggregator;
import com.hazelcast.jet.memory.api.operations.aggregator.AggregationIterator;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.impl.operations.aggregator.PartitionedAggregator;
import com.hazelcast.jet.memory.impl.operations.joiner.iterator.InMemoryIterator;
import com.hazelcast.jet.memory.impl.operations.joiner.iterator.SpillingIterator;
import com.hazelcast.jet.memory.api.memory.spilling.spillers.KeyValueStorageSpiller;
import com.hazelcast.jet.memory.impl.memory.impl.spilling.spillers.KeyValueStorageSpillerImpl;

import java.util.Iterator;

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
public class PartitionedJoiner<T> extends PartitionedAggregator<T>
        implements JoinAggregator<T> {
    private short source = 0;
    private final short sourceCount;

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public PartitionedJoiner(
            short sourceCount,
            int partitionCount,
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
        super(
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
                spillingDirectory,
                spillingChunkSize,
                spillToDisk,
                useBigEndian
        );

        this.sourceCount = sourceCount;
    }


    @Override
    public Iterator<T> iterator() {
        iterator.reset(getComparator());
        spillingKeyValueReader = this.spiller.openSpillingReader();
        return iterator;
    }

    @Override
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
    protected AggregationIterator<T> createResultIterator() {
        return new DefaultJoinerIterator();
    }

    @Override
    public void setSource(short source) {
        this.source = source;
    }

    private class DefaultJoinerIterator implements AggregationIterator<T> {
        private boolean spilledDone;
        private final AggregationIterator<T> memoryIterator;
        private final AggregationIterator<T> spillingIterator;

        public DefaultJoinerIterator() {
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
                            sourceCount,
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
