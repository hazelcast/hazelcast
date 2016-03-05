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

package com.hazelcast.jet.memory.impl.operations.joiner.sorted;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.spi.operations.aggregator.SortedJoinAggregator;
import com.hazelcast.jet.memory.impl.operations.aggregator.sorted.PartitionedSortedAggregator;

public class PartitionedSortedJoiner<T> extends PartitionedSortedAggregator<T>
        implements SortedJoinAggregator<T> {
    private short source;
    private final short sourceCount;

    public PartitionedSortedJoiner(
            short sourceCount,
            int partitionCount,
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
            boolean useBigEndian
    ) {
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
                spillingDirectory,
                orderingDirection,
                spillingChunkSize,
                spillToDisk,
                useBigEndian
        );

        this.sourceCount = sourceCount;
    }

    @Override
    public void setSource(short source) {
        this.source = source;
    }
}
