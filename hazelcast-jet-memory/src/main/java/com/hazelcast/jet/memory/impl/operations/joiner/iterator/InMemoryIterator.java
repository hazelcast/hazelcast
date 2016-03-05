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

package com.hazelcast.jet.memory.impl.operations.joiner.iterator;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.api.binarystorage.BloomFilter;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;

public class InMemoryIterator<T>
        extends com.hazelcast.jet.memory.impl.operations.aggregator.iterator.InMemoryIterator<T> {
    private final int sourceCount;

    public InMemoryIterator(
            BinaryKeyValueStorage serviceKeyValueStorage,
            MemoryBlock serviceMemoryBlock,
            MemoryBlock temporaryMemoryBlock,
            BinaryFunctor functor,
            ContainersPull<T> containersPull,
            KeyValueDataPartition[] partitions,
            StorageHeader header,
            ElementsWriter<T> keysWriter,
            ElementsWriter<T> valuesWriter,
            BloomFilter bloomFilter,
            IOContext ioContext,
            int sourceCount,
            boolean useBigEndian
    ) {
        super(
                serviceKeyValueStorage,
                serviceMemoryBlock,
                temporaryMemoryBlock,
                functor,
                containersPull,
                partitions,
                header,
                keysWriter,
                valuesWriter,
                bloomFilter,
                ioContext,
                useBigEndian
        );

        this.sourceCount = sourceCount;
    }

    @Override
    protected T readNext() {
        return null;
    }

    @Override
    protected boolean checkHasNext() {
        return checkMemoryBlock()
                || checkPartition();
    }
}
