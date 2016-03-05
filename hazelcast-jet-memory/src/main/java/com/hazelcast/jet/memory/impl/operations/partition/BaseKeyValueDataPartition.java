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

package com.hazelcast.jet.memory.impl.operations.partition;

import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.impl.memory.impl.management.memorychain.DefaultMemoryBlockChain;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseKeyValueDataPartition<T, S extends BinaryKeyValueStorage<T>>
        implements KeyValueDataPartition<T> {
    private final int partitionId;

    protected final S binaryKeyValueStorage;

    private final MemoryContext memoryContext;

    private final MemoryBlockChain memoryBlockChain;

    private final MemoryChainingType memoryChainingType;

    private final List<MemoryBlockChain> sourceChains =
            new ArrayList<MemoryBlockChain>();

    protected BaseKeyValueDataPartition(int partitionId,
                                        MemoryContext memoryContext,
                                        MemoryChainingType memoryChainingType,
                                        S binaryKeyValueStorage) {
        this.partitionId = partitionId;
        this.binaryKeyValueStorage = binaryKeyValueStorage;
        this.memoryContext = memoryContext;
        this.memoryChainingType = memoryChainingType;
        this.memoryBlockChain = createMemoryBlockChain(memoryContext, memoryChainingType);
    }

    private DefaultMemoryBlockChain createMemoryBlockChain(
            MemoryContext memoryContext,
            MemoryChainingType memoryChainingType) {
        return new DefaultMemoryBlockChain(
                memoryContext,
                true,
                memoryChainingType
        );
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public MemoryBlockChain getMemoryBlockChain() {
        return memoryBlockChain;
    }

    @Override
    public MemoryBlockChain getMemoryBlockChain(int sourceId) {
        return sourceChains.get(sourceId);
    }

    @Override
    public void initNextSource() {
        sourceChains.add(createMemoryBlockChain(
                memoryContext,
                memoryChainingType
        ));
    }

    @Override
    public S getKeyValueStorage() {
        return binaryKeyValueStorage;
    }
}
