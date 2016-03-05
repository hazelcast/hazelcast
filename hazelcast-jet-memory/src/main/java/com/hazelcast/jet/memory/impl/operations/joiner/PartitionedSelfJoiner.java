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
import com.hazelcast.jet.memory.impl.operations.aggregator.PartitionedAggregator;

public class PartitionedSelfJoiner<T> extends PartitionedAggregator<T>
        implements JoinAggregator<T> {
    private final int numSelfJoins;

    public PartitionedSelfJoiner(
            int numSelfJoins,
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
            boolean useBigEndian
    ) {
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

        this.numSelfJoins = numSelfJoins;
    }

    @Override
    public void setSource(short source) {
        assert source == 0;
    }
}
