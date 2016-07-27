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

package com.hazelcast.jet.memory.operation.aggregator.cursor;

import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.PairFetcher;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;

/**
 * Base class for pair cursor implementations.
 */
public abstract class PairCursorBase implements PairCursor {

    protected final boolean useBigEndian;
    protected final StorageHeader header;
    protected final Accumulator accumulator;
    protected final MemoryBlock serviceMemoryBlock;
    protected final Pair destPair;
    protected final MemoryBlock temporaryMemoryBlock;
    protected final Partition[] partitions;
    protected final PairFetcher pairFetcher;

    protected Comparator comparator;

    protected PairCursorBase(
            MemoryBlock serviceMemoryBlock, MemoryBlock temporaryMemoryBlock, Accumulator accumulator,
            Pair destPair, Partition[] partitions, StorageHeader header, SerializationOptimizer optimizer,
            boolean useBigEndian
    ) {
        this.header = header;
        this.partitions = partitions;
        this.useBigEndian = useBigEndian;
        this.accumulator = accumulator;
        this.destPair = destPair;
        this.serviceMemoryBlock = serviceMemoryBlock;
        this.temporaryMemoryBlock = temporaryMemoryBlock;
        this.pairFetcher = new PairFetcher(optimizer, this.destPair, useBigEndian);
    }

    @Override
    public Pair asPair() {
        return pairFetcher.pair();
    }

    @Override
    public void reset(Comparator comparator) {
        this.comparator = comparator;
    }
}
