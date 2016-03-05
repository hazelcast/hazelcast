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

package com.hazelcast.jet.memory.impl.operations.aggregator.iterator.sorted;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.spi.operations.BinaryDataFetcher;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.DataSorter;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.InputsIterator;
import com.hazelcast.jet.memory.impl.operations.aggregator.iterator.BaseAggregatorIterator;

public class SortedAggregationIterator<T> extends BaseAggregatorIterator<T> {
    private boolean lastSortResult;
    private final InputsIterator inputsIterator;
    private final DataSorter<InputsIterator, BinaryDataFetcher<T>> memoryDiskMergeSorter;

    public SortedAggregationIterator(
            MemoryBlock serviceMemoryBlock,
            MemoryBlock temporaryMemoryBlock,
            DataSorter<InputsIterator, BinaryDataFetcher<T>>
                    memoryDiskMergeSorter,
            BinaryFunctor binaryFunctor,
            ContainersPull<T> containersPull,
            KeyValueDataPartition[] partitions,
            StorageHeader header,
            ElementsWriter<T> keyWriter,
            ElementsWriter<T> valueWriter,
            IOContext ioContext,
            InputsIterator inputsIterator,
            boolean useBigEndian
    ) {
        super(
                serviceMemoryBlock,
                temporaryMemoryBlock,
                binaryFunctor,
                containersPull,
                partitions,
                header,
                keyWriter,
                valueWriter,
                null,
                ioContext,
                useBigEndian
        );

        this.inputsIterator = inputsIterator;
        this.memoryDiskMergeSorter = memoryDiskMergeSorter;
    }

    @Override
    protected boolean checkHasNext() {
        if (lastSortResult) {
            return false;
        }

        binaryDataFetcher.reset();
        lastSortResult = memoryDiskMergeSorter.sort();
        return !lastSortResult
                || binaryDataFetcher.getCurrentContainer() != null;
    }

    @Override
    protected T readNext() {
        hasNextCalled = false;
        return binaryDataFetcher.getCurrentContainer();
    }

    public void reset(BinaryComparator comparator) {
        super.reset(comparator);
        binaryDataFetcher.reset();
        lastSortResult = false;
        memoryDiskMergeSorter.open(inputsIterator, binaryDataFetcher);
    }
}
