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

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.binarystorage.SortOrder;
import com.hazelcast.jet.memory.binarystorage.SortedStorage;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.memoryblock.DefaultMemoryBlockChain;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;

/**
 * This is abstract implementation of chain of the blocks;
 * <p>
 * If have blocks with data:
 * <p>
 * <pre>
 *     Block1   Block2  ......... BlockN
 * </pre>
 * <p>
 * it sorts data inside each block and return result chain of blocks with sorted data;
 */
public class MemoryBlockSorter implements Sorter<Partition[], MemoryBlockChain> {
    /**
     * Array of all source partitions.
     */
    private Partition[] partitions;

    /**
     * ID of the partition currently being sorted.
     */
    private int partitionId;

    /**
     * The partition currently being sorted.
     */
    private Partition partition;

    /**
     * ID of current memory block.
     */
    private int memoryBlockId;

    /**
     * Storage header to obtain the address of the OA-storage.
     */
    private final StorageHeader storageHeader;

    /**
     * Contains a list of memory blocks with sorted storage.
     */
    private MemoryBlockChain resultChain;

    /**
     * Sort order (ascending, descending).
     */
    private final SortOrder order;

    /**
     * In-flight object.
     */
    private SortedStorage sortedStorage;

    public MemoryBlockSorter(StorageHeader storageHeader, SortOrder order) {
        this.order = order;
        this.storageHeader = storageHeader;
        this.resultChain = new DefaultMemoryBlockChain();
    }

    @Override
    public void resetTo(Partition[] input, MemoryBlockChain output) {
        reset();
        partitions = input;
        resultChain = output;
    }

    @Override
    public boolean sort() {
        if (!findMemoryBlockToSort() && !findPartitionToSort()) {
            return true;
        }
        sortedStorage.ensureSorted(order);
        return false;
    }

    private boolean findPartitionToSort() {
        if (partitionId > partitions.length - 1) {
            partitionId = 0;
            return false;
        }
        memoryBlockId = 0;
        partition = partitions[partitionId];
        sortedStorage = (SortedStorage) partition.getStorage();
        partitionId++;
        return findMemoryBlockToSort();
    }

    private boolean findMemoryBlockToSort() {
        if (partition == null) {
            return false;
        }
        if (memoryBlockId > partition.getMemoryBlockChain().size() - 1) {
            return false;
        }
        for (int blockIdx = memoryBlockId; blockIdx < partition.getMemoryBlockChain().size(); blockIdx++) {
            MemoryBlock memoryBlock = partition.getMemoryBlockChain().get(blockIdx);
            storageHeader.setMemoryBlock(memoryBlock);
            if (storageHeader.baseAddress() != MemoryAllocator.NULL_ADDRESS) {
                memoryBlockId = blockIdx + 1;
                sortedStorage.setMemoryBlock(memoryBlock);
                sortedStorage.gotoAddress(storageHeader.baseAddress());
                if (sortedStorage.count() > 0) {
                    resultChain.add(memoryBlock);
                }
                return true;
            }
        }
        memoryBlockId = partition.getMemoryBlockChain().size();
        return false;
    }

    private void reset() {
        partitionId = 0;
        partition = null;
        memoryBlockId = 0;
        resultChain.clear();
    }
}
