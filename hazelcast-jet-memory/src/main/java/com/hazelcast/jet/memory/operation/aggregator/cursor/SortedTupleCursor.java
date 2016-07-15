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

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.TupleFetcher;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.operation.aggregator.sorter.Sorter;

/**
 * Cursor that encounters tuples in a given sort order.
 */
public class SortedTupleCursor extends TupleCursorBase {
    private boolean done;
    private final InputsCursor inputsCursor;
    private final Sorter<InputsCursor, TupleFetcher> memoryDiskMergeSorter;

    public SortedTupleCursor(
            MemoryBlock serviceMemoryBlock, MemoryBlock temporaryMemoryBlock,
            Sorter<InputsCursor, TupleFetcher> memoryDiskMergeSorter,
            Accumulator accumulator, Tuple2 destTuple, Partition[] partitions, StorageHeader header,
            IOContext ioContext, InputsCursor inputsCursor, boolean useBigEndian
    ) {
        super(serviceMemoryBlock, temporaryMemoryBlock, accumulator, destTuple, partitions, header,
                ioContext, useBigEndian);
        this.inputsCursor = inputsCursor;
        this.memoryDiskMergeSorter = memoryDiskMergeSorter;
    }

    @Override
    public boolean advance() {
        assert !done : "Cursor is invalid";
        done = memoryDiskMergeSorter.sort();
        return !done;
    }

    @Override
    public void reset(Comparator comparator) {
        super.reset(comparator);
        done = false;
        memoryDiskMergeSorter.resetTo(inputsCursor, tupleFetcher);
    }
}
