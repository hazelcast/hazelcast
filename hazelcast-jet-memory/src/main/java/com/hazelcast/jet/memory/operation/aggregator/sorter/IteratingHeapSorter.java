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

package com.hazelcast.jet.memory.operation.aggregator.sorter;

import com.hazelcast.jet.memory.PairFetcher;
import com.hazelcast.jet.memory.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.binarystorage.SortOrder;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.operation.aggregator.cursor.InputsCursor;

/**
 * Iterating heap sorter ??
 */
public class IteratingHeapSorter extends AbstractHeapSorter<PairFetcher> {
    private static final int CHUNK_SIZE = 1;

    public IteratingHeapSorter(
            MemoryBlock temporaryMemoryBlock, SortOrder direction, Comparator comparator,
            ObjectHolder<Comparator> comparatorHolder, Accumulator accumulator, boolean useBigEndian
    ) {
        super(CHUNK_SIZE, direction, comparatorHolder, comparator, accumulator, temporaryMemoryBlock, useBigEndian);
    }

    @Override
    protected boolean applyNonAssociativeAccumulator() {
        return true;
    }

    @Override
    protected void outputSlot(InputsCursor iterator, int inputId) {
    }

    @Override
    protected void outputSegment(int sourceId, long recordsCount) {
    }

    @Override
    protected void outputPair(MemoryBlock memoryBlock, long pairAddress) {
        output.fetch(memoryBlock, pairAddress);
    }

}
