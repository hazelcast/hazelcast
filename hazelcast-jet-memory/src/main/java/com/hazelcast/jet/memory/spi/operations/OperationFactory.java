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

package com.hazelcast.jet.memory.spi.operations;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.impl.operations.aggregator.PartitionedAggregator;
import com.hazelcast.jet.memory.impl.operations.aggregator.sorted.PartitionedSortedAggregator;
import com.hazelcast.jet.memory.impl.operations.joiner.PartitionedJoiner;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.operations.aggregator.Aggregator;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.operations.aggregator.JoinAggregator;
import com.hazelcast.jet.memory.spi.operations.aggregator.SortedJoinAggregator;
import com.hazelcast.jet.memory.spi.operations.aggregator.sorting.SortedAggregator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;

public final class OperationFactory {
    private OperationFactory() {
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static <T> Aggregator<T> getAggregator(
            MemoryContext memoryContext,
            IOContext ioContext,
            MemoryChainingType memoryChainingType,
            int partitionCount,
            int spillingBufferSize,
            BinaryComparator binaryComparator,
            ElementsWriter<T> keyWriter,
            ElementsWriter<T> valueWriter,
            ContainersPull<T> containersPull,
            String spillingDirectory,
            int spillingChunkSize,
            int bloomFilterSizeInBytes,
            boolean spillToDisk,
            boolean useBigEndian
    ) {
        return new PartitionedAggregator<T>(
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
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static <T> Aggregator<T> getAggregator(
            MemoryContext memoryContext,
            IOContext ioContext,
            MemoryChainingType memoryChainingType,
            int partitionCount,
            int spillingBufferSize,
            BinaryComparator binaryComparator,
            ElementsWriter<T> keyWriter,
            ElementsWriter<T> valueWriter,
            ContainersPull<T> containersPull,
            BinaryFunctor binaryFunctor,
            String spillingDirectory,
            int spillingChunkSize,
            int bloomFilterSizeInBytes,
            boolean spillToDisk,
            boolean useBigEndian
    ) {
        return new PartitionedAggregator<T>(
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
                binaryFunctor,
                spillingDirectory,
                spillingChunkSize,
                spillToDisk,
                useBigEndian
        );
    }


    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static <T> SortedAggregator<T> getSortedAggregator(
            MemoryContext memoryContext,
            IOContext ioContext,
            MemoryChainingType memoryChainingType,
            int partitionCount,
            int spillingBufferSize,
            BinaryComparator binaryComparator,
            ElementsWriter<T> keyWriter,
            ElementsWriter<T> valueWriter,
            ContainersPull<T> containersPull,
            String spillingDirectory,
            OrderingDirection orderingDirection,
            int spillingChunkSize,
            boolean spillToDisk,
            boolean useBigEndian
    ) {
        return new PartitionedSortedAggregator<T>(
                partitionCount,
                spillingBufferSize,
                ioContext,
                binaryComparator,
                memoryContext,
                memoryChainingType,
                keyWriter,
                valueWriter,
                containersPull,
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
    public static <T> SortedAggregator<T> getSortedAggregator(
            MemoryContext memoryContext,
            IOContext ioContext,
            MemoryChainingType memoryChainingType,
            int partitionCount,
            int spillingBufferSize,
            BinaryComparator binaryComparator,
            ElementsWriter<T> keyWriter,
            ElementsWriter<T> valueWriter,
            ContainersPull<T> containersPull,
            BinaryFunctor binaryFunctor,
            String spillingDirectory,
            OrderingDirection orderingDirection,
            int spillingChunkSize,
            boolean spillToDisk,
            boolean useBigEndian
    ) {
        return new PartitionedSortedAggregator<T>(
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
                orderingDirection,
                spillingChunkSize,
                spillToDisk,
                useBigEndian
        );
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static <T> JoinAggregator<T> getJoiner(
            MemoryContext memoryContext,
            IOContext ioContext,
            MemoryChainingType memoryChainingType,
            int partitionCount,
            int spillingBufferSize,
            BinaryComparator binaryComparator,
            ElementsWriter<T> keyWriter,
            ElementsWriter<T> valueWriter,
            ContainersPull<T> containersPull,
            String spillingDirectory,
            int spillingChunkSize,
            int bloomFilterSizeInBytes,
            boolean spillToDisk,
            boolean useBigEndian
    ) {
        return new PartitionedJoiner<T>(
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
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static <T> SortedJoinAggregator<T> getSortedJoiner(
            MemoryContext memoryContext,
            IOContext ioContext,
            MemoryChainingType memoryChainingType,
            int partitionCount,
            int spillingBufferSize,
            BinaryComparator binaryComparator,
            ElementsWriter<T> keyWriter,
            ElementsWriter<T> valueWriter,
            ContainersPull<T> containersPull,
            String spillingDirectory,
            OrderingDirection orderingDirection,
            int spillingChunkSize,
            boolean spillToDisk,
            boolean useBigEndian
    ) {
        return null;
    }
}
