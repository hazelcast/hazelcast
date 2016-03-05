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

import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.impl.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.spi.operations.BinaryDataFetcher;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.InputsIterator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;

import java.io.IOException;

public class IteratingHeapSorter<T>
        extends AbstractHeapSorter<BinaryDataFetcher<T>> {
    private static final int CHUNK_SIZE = 1;

    private final ElementsWriter<T> keyWriter;
    private final ElementsWriter<T> valueWriter;

    public IteratingHeapSorter(MemoryBlock temporaryMemoryBlock,
                               ElementsWriter<T> keyWriter,
                               ElementsWriter<T> valueWriter,
                               OrderingDirection direction,
                               BinaryComparator binaryComparator,
                               ObjectHolder<BinaryComparator> comparatorHolder,
                               BinaryFunctor binaryFunctor,
                               boolean useBigEndian
    ) {
        super(
                CHUNK_SIZE,
                direction,
                comparatorHolder,
                binaryComparator,
                binaryFunctor,
                temporaryMemoryBlock,
                useBigEndian
        );

        this.keyWriter = keyWriter;
        this.valueWriter = valueWriter;
    }

    @Override
    protected boolean applyNonAssociateFunctor() {
        return true;
    }

    @Override
    protected void writeSlotToOut(InputsIterator iterator, int inputId) {

    }

    @Override
    protected void writeSourceToOut(int sourceId, long recordsCount) {

    }

    @Override
    protected void writeRecordToOut(MemoryBlock memoryBlock,
                                    long recordAddress) {
        output.prepareContainerForNextRecord();
        readNextRecord(memoryBlock, recordAddress, keyWriter, valueWriter);
    }

    private void readNextRecord(MemoryBlock memoryBlock,
                                long recordAddress,
                                ElementsWriter<T> keyWriter,
                                ElementsWriter<T> valueWriter) {
        try {
            output.prepareContainerForNextRecord();
            keyWriter.setSource(output.getCurrentContainer());
            valueWriter.setSource(output.getCurrentContainer());

            output.fetchRecord(
                    memoryBlock,
                    recordAddress,
                    keyWriter,
                    valueWriter
            );
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }
}
