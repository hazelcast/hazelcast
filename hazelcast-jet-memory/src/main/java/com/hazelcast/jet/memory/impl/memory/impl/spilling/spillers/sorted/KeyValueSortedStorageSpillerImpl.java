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

package com.hazelcast.jet.memory.impl.memory.impl.spilling.spillers.sorted;

import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.DataSorter;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.InputsIterator;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueWriter;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueSortedDataPartition;
import com.hazelcast.jet.memory.api.memory.spilling.spillers.KeyValueSortedStorageSpiller;
import com.hazelcast.jet.memory.impl.memory.impl.spilling.spillers.BaseKeyValueSpillerImpl;
import com.hazelcast.jet.memory.impl.util.Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;

public class KeyValueSortedStorageSpillerImpl<T>
        extends BaseKeyValueSpillerImpl<T, KeyValueSortedDataPartition<T>>
        implements KeyValueSortedStorageSpiller<T> {
    private boolean storageSorted;

    private final InputsIterator inputsIterator;

    private final MemoryBlockChain sortedMemoryBlockChain;

    private final DataSorter<KeyValueSortedDataPartition[], MemoryBlockChain>
            memoryBlocksSorter;

    private final DataSorter<InputsIterator, SpillingKeyValueWriter> spillingSorter;

    public KeyValueSortedStorageSpillerImpl(
            int spillingBufferSize,
            File spillingDirectory,
            MemoryBlockChain sortedMemoryBlockChain,
            DataSorter<KeyValueSortedDataPartition[], MemoryBlockChain>
                    memoryBlocksSorter,
            DataSorter<InputsIterator, SpillingKeyValueWriter> spillingSorter,
            InputsIterator inputsIterator,
            boolean useBigEndian
    ) {
        super(spillingBufferSize, spillingDirectory, useBigEndian);
        this.spillingSorter = spillingSorter;
        this.inputsIterator = inputsIterator;
        this.memoryBlocksSorter = memoryBlocksSorter;
        this.sortedMemoryBlockChain = sortedMemoryBlockChain;
    }

    @Override
    public void start(KeyValueSortedDataPartition[] partitions) {
        memoryBlocksSorter.open(partitions, sortedMemoryBlockChain);
        storageSorted = false;

        checkSpillingFile();

        try {
            spillingDataInput.open(new FileInputStream(activeSpillingFile));
            spillingDataOutput.open(new FileOutputStream(temporarySpillingFile));
            spillingKeyValueReader.open(spillingDataInput);
            spillingKeyValueWriter.open(spillingDataOutput);
        } catch (FileNotFoundException e) {
            throw Util.reThrow(e);
        }
    }

    @Override
    public boolean processNextChunk() {
        if (!storageSorted) {
            if (!memoryBlocksSorter.sort()) {
                return false;
            }

            inputsIterator.setInputs(
                    openSpillingReader(),
                    sortedMemoryBlockChain
            );
            spillingSorter.open(inputsIterator, spillingKeyValueWriter);
            storageSorted = true;
        }

        return spillingSorter.sort();
    }

    @Override
    public void stop() {
        super.stop();
        sortedMemoryBlockChain.clear();
    }
}
