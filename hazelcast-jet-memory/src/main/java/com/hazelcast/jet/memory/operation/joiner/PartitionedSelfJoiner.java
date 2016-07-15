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

/**
 * Partitioned self-joiner
 */
public class PartitionedSelfJoiner extends PartitionedAggregator implements JoinAggregator {

    @SuppressWarnings("checkstyle:parameternumber")
    public PartitionedSelfJoiner(
            int partitionCount, int spillingBufferSize, IOContext ioContext,
            Comparator comparator, MemoryContext memoryContext, MemoryChainingRule memoryChainingRule,
            Tuple2 tuple, String spillingDirectory, int spillingChunkSize,
            boolean spillToDisk, boolean useBigEndian
    ) {
        super(partitionCount, spillingBufferSize, ioContext, comparator, memoryContext,
                memoryChainingRule, tuple, spillingDirectory, spillingChunkSize, spillToDisk,
                useBigEndian);
    }

    @Override
    public void setSource(short source) {
        assert source == 0;
    }
}
