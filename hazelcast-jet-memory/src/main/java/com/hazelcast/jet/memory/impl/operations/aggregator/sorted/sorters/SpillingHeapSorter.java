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

package com.hazelcast.jet.memory.impl.operations.aggregator.sorted.sorters;

import com.hazelcast.jet.memory.impl.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.InputsIterator;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueWriter;

public class SpillingHeapSorter
        extends AbstractHeapSorter<SpillingKeyValueWriter> {
    public SpillingHeapSorter(int chunkSize,
                              OrderingDirection direction,
                              ObjectHolder<BinaryComparator> comparatorHolder,
                              BinaryComparator binaryComparator,
                              BinaryFunctor binaryFunctor,
                              MemoryBlock temporaryMemoryBlock,
                              boolean useBigEndian) {
        super(
                chunkSize,
                direction,
                comparatorHolder,
                binaryComparator,
                binaryFunctor,
                temporaryMemoryBlock,
                useBigEndian
        );
    }

    @Override
    protected boolean applyNonAssociateFunctor() {
        return false;
    }

    @Override
    protected void writeSlotToOut(InputsIterator iterator,
                                  int inputId) {
        output.writeSlotHeader(
                iterator.partitionId(inputId),
                iterator.getHashCode(inputId),
                1
        );
    }

    @Override
    protected void writeSourceToOut(int sourceId,
                                    long recordsCount) {
        output.writeSourceHeader(sourceId, recordsCount);
    }

    @Override
    protected void writeRecordToOut(MemoryBlock memoryBlock,
                                    long recordAddress) {
        output.writeRecord(memoryBlock, recordAddress);
    }
}
