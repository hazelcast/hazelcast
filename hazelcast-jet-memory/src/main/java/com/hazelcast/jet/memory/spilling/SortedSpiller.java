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

package com.hazelcast.jet.memory.spilling;

import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.operation.aggregator.cursor.InputsCursor;
import com.hazelcast.jet.memory.operation.aggregator.sorter.Sorter;
import com.hazelcast.jet.memory.util.Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/**
 * Sorted spiller.
 */
public class SortedSpiller extends SpillerBase implements Spiller {

    private boolean storageSorted;

    private final InputsCursor inputsCursor;

    private final MemoryBlockChain sortedMemoryBlockChain;

    private final Sorter<Partition[], MemoryBlockChain> memoryBlocksSorter;

    private final Sorter<InputsCursor, SpillingKeyValueWriter> spillingSorter;

    public SortedSpiller(
            int spillingBufferSize,
            File spillingDirectory,
            MemoryBlockChain sortedMemoryBlockChain,
            Sorter<Partition[], MemoryBlockChain> memoryBlocksSorter,
            Sorter<InputsCursor, SpillingKeyValueWriter> spillingSorter,
            InputsCursor inputsCursor,
            boolean useBigEndian
    ) {
        super(spillingDirectory, spillingBufferSize, useBigEndian);
        this.spillingSorter = spillingSorter;
        this.inputsCursor = inputsCursor;
        this.memoryBlocksSorter = memoryBlocksSorter;
        this.sortedMemoryBlockChain = sortedMemoryBlockChain;
    }

    @Override
    public void start(Partition[] partitions) {
        memoryBlocksSorter.resetTo(partitions, sortedMemoryBlockChain);
        storageSorted = false;

        ensureSpillFiles();

        try {
            input.setInput(new FileInputStream(activeFile));
            output.setOutput(new FileOutputStream(tempFile));
            spillFileCursor.open(input);
            recordWriter.open(output);
        } catch (FileNotFoundException e) {
            throw Util.rethrow(e);
        }
    }

    @Override
    public boolean processNextChunk() {
        if (!storageSorted) {
            if (!memoryBlocksSorter.sort()) {
                return true;
            }
            inputsCursor.setInputs(openSpillFileCursor(), sortedMemoryBlockChain);
            spillingSorter.resetTo(inputsCursor, recordWriter);
            storageSorted = true;
        }

        return !spillingSorter.sort();
    }

    @Override
    public void stop() {
        super.stop();
        sortedMemoryBlockChain.clear();
    }

}
