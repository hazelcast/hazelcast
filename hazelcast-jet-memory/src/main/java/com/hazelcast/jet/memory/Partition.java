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

package com.hazelcast.jet.memory;

import com.hazelcast.jet.memory.binarystorage.HashStorage;
import com.hazelcast.jet.memory.binarystorage.SortedHashStorage;
import com.hazelcast.jet.memory.binarystorage.Storage;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.DefaultMemoryBlockChain;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

/**
 * Holds a slice of a multimap, based on the key's hashcode.
 */
public class Partition {
    private final int partitionId;

    private final Storage storage;
    private final MemoryContext memoryContext;
    private final MemoryBlockChain memoryBlockChain;
    private final MemoryChainingRule memoryChainingRule;
    private final List<MemoryBlockChain> sourceChains = new ArrayList();

    Partition(int partitionId, MemoryContext memoryContext,
              MemoryChainingRule memoryChainingRule, Storage storage
    ) {
        this.partitionId = partitionId;
        this.storage = storage;
        this.memoryContext = memoryContext;
        this.memoryChainingRule = memoryChainingRule;
        this.memoryBlockChain = createMemoryBlockChain(memoryContext, memoryChainingRule);
    }

    public static  Partition newPartition(
            int partitionId, MemoryContext memoryContext, MemoryChainingRule memoryChainingRule,
            Comparator defaultComparator, LongConsumer hsaResizeListener
    ) {
        return new Partition(partitionId, memoryContext, memoryChainingRule,
                new HashStorage(null, defaultComparator.getHasher(), hsaResizeListener));
    }

    public static  Partition newSortedPartition(
            int partitionId, MemoryContext memoryContext, MemoryChainingRule memoryChainingRule,
            Comparator defaultComparator, LongConsumer hsaResizeListener
    ) {
        return new Partition(partitionId, memoryContext, memoryChainingRule,
                new SortedHashStorage(null, defaultComparator, hsaResizeListener));
    }

    public int getPartitionId() {
        return partitionId;
    }

    public MemoryBlockChain getMemoryBlockChain() {
        return memoryBlockChain;
    }

    public MemoryBlockChain getMemoryBlockChain(int sourceId) {
        return sourceChains.get(sourceId);
    }

    public void initNextSource() {
        sourceChains.add(createMemoryBlockChain(memoryContext, memoryChainingRule));
    }

    public Storage getStorage() {
        return storage;
    }

    private static DefaultMemoryBlockChain createMemoryBlockChain(
            MemoryContext memoryContext, MemoryChainingRule memoryChainingRule
    ) {
        return new DefaultMemoryBlockChain(memoryContext, true, memoryChainingRule);
    }
}
