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

package com.hazelcast.jet.memory.operation;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.binarystorage.SortOrder;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.operation.aggregator.Aggregator;
import com.hazelcast.jet.memory.operation.aggregator.JoinAggregator;
import com.hazelcast.jet.memory.operation.aggregator.PartitionedAggregator;
import com.hazelcast.jet.memory.operation.aggregator.SortedAggregator;
import com.hazelcast.jet.memory.operation.aggregator.SortedPartitionedAggregator;
import com.hazelcast.jet.memory.operation.joiner.PartitionedJoiner;

/**
 * Entry point to the Jet Memory module: a factory of operation objects.
 */
public final class OperationFactory {
    private OperationFactory() {
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static Aggregator getAggregator(
            MemoryContext memoryContext, IOContext ioContext, MemoryChainingRule memoryChainingRule,
            int partitionCount, int spillingBufferSize, Comparator comparator, Tuple2 destTuple,
            String spillingDirectory, int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        return new PartitionedAggregator(partitionCount, spillingBufferSize, ioContext,
                comparator, memoryContext, memoryChainingRule, destTuple, spillingDirectory,
                spillingChunkSize, spillToDisk, useBigEndian);
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static Aggregator getAggregator(
            MemoryContext memoryContext, IOContext ioContext, MemoryChainingRule memoryChainingRule,
            int partitionCount, int spillingBufferSize, Comparator comparator, Tuple2 destTuple,
            Accumulator accumulator, String spillingDirectory,
            int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        return new PartitionedAggregator(partitionCount, spillingBufferSize, ioContext,
                comparator, memoryContext, memoryChainingRule, destTuple, accumulator,
                spillingDirectory, spillingChunkSize, spillToDisk, useBigEndian);
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static SortedAggregator getSortedAggregator(
            MemoryContext memoryContext, IOContext ioContext, MemoryChainingRule memoryChainingRule,
            int partitionCount, int spillingBufferSize, Comparator comparator, Tuple2 destTuple,
            String spillingDirectory, SortOrder sortOrder, int spillingChunkSize,
            boolean spillToDisk, boolean useBigEndian
    ) {
        return new SortedPartitionedAggregator(partitionCount, spillingBufferSize, ioContext, comparator,
                memoryContext, memoryChainingRule, destTuple, spillingDirectory, sortOrder,
                spillingChunkSize, spillToDisk, useBigEndian);
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static SortedAggregator getSortedAggregator(
            MemoryContext memoryContext, IOContext ioContext, MemoryChainingRule memoryChainingRule,
            int partitionCount, int spillingBufferSize, Comparator comparator, Tuple2 destTuple,
            Accumulator binaryFunctor, String spillingDirectory, SortOrder sortOrder,
            int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        return new SortedPartitionedAggregator(partitionCount, spillingBufferSize, ioContext, comparator,
                memoryContext, memoryChainingRule, destTuple, binaryFunctor, spillingDirectory,
                sortOrder, spillingChunkSize, spillToDisk, useBigEndian);
    }

    @SuppressWarnings({
            "checkstyle:parameternumber"
    })
    public static  JoinAggregator getJoiner(
            MemoryContext memoryContext, IOContext ioContext, MemoryChainingRule memoryChainingRule,
            int partitionCount, int spillingBufferSize, Comparator comparator, Tuple2 tuple,
            String spillingDirectory, int spillingChunkSize, boolean spillToDisk, boolean useBigEndian
    ) {
        return new PartitionedJoiner(partitionCount, spillingBufferSize, ioContext, comparator, memoryContext,
                memoryChainingRule, tuple, spillingDirectory, spillingChunkSize, spillToDisk, useBigEndian);
    }

//    @SuppressWarnings({
//            "checkstyle:parameternumber"
//    })
//    public static  SortedJoinAggregator getSortedJoiner(
//            MemoryContext memoryContext, IOContext ioContext, MemoryChainingType memoryChainingType,
//            int partitionCount, int spillingBufferSize, Comparator comparator,
//            TupleUpdater tupleUpdater, TuplePooltuplePool,
//            String spillingDirectory, SortOrder sortOrder, int spillingChunkSize,
//            boolean spillToDisk, boolean useBigEndian
//    ) {
//        return null;
//    }
}
