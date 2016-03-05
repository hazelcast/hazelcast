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
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.api.binarystorage.BloomFilter;
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.api.memory.spilling.spillers.KeyValueStorageSpiller;

public class SpillingIterator<T, P extends KeyValueDataPartition<T>>
        extends com.hazelcast.jet.memory.impl.operations.aggregator.iterator.SpillingIterator<T, P> {
    public SpillingIterator(
            MemoryBlock serviceMemoryBlock,
            MemoryBlock temporaryMemoryBlock,
            BinaryFunctor functor,
            KeyValueStorageSpiller<T, P> spiller,
            ContainersPull<T> containersPull,
            KeyValueDataPartition[] partitions,
            StorageHeader header,
            ElementsWriter<T> keysWriter,
            ElementsWriter<T> valuesWriter,
            BloomFilter bloomFilter,
            IOContext ioContext,
            boolean useBigEndian) {
        super(
                serviceMemoryBlock,
                temporaryMemoryBlock,
                functor,
                spiller,
                containersPull,
                partitions,
                header,
                keysWriter,
                valuesWriter,
                bloomFilter,
                ioContext,
                useBigEndian
        );
    }
}
