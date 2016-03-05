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

package com.hazelcast.jet.memory.impl.operations.aggregator.sorted;

import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.DataSorter;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueSortedDataPartition;
import com.hazelcast.jet.memory.api.binarystorage.sorted.BinaryKeyValueSortedStorage;
import com.hazelcast.jet.memory.impl.memory.impl.management.memorychain.DefaultMemoryBlockChain;

/**
 * This is abstract implementation of chain of the blocks;
 * <p>
 * If if have blocks with data:
 * <p>
 * <pre>
 *     Block1   Block2  ......... BlockN
 * </pre>
 * <p>
 * it sorts data inside each block and return result chain of blocks with sorted data;
 */
public class DefaultMemoryBlocksSorter
        implements DataSorter<KeyValueSortedDataPartition[], MemoryBlockChain> {
    /**
     * Currently sorted partition id;
     */
    private int partitionId;

    /**
     * Id of current memory block;
     */
    private int memoryBlockId;

    /**
     * Storage header to obtain address of the OA-storage;
     */
    private final StorageHeader storageHeader;

    /**
     * Contains list of memory-blocks with sorted storage;
     */
    private MemoryBlockChain resultChain;

    /**
     * Direction of sorting (asc,desc);
     */
    private final OrderingDirection direction;

    /**
     * Currently sorted partitions;
     */
    private KeyValueSortedDataPartition partition;

    /**
     * Source partitions;
     */
    private KeyValueSortedDataPartition[] partitions;

    /**
     * On-flight object;
     */
    private BinaryKeyValueSortedStorage binaryKeyValueSortedStorage;

    public DefaultMemoryBlocksSorter(
            StorageHeader storageHeader,
            OrderingDirection direction
    ) {
        this.direction = direction;
        this.storageHeader = storageHeader;
        this.resultChain = new DefaultMemoryBlockChain();
    }

    @Override
    public void open(KeyValueSortedDataPartition[] input,
                     MemoryBlockChain output) {
        reset();
        partitions = input;
        resultChain = output;
    }

    @Override
    public boolean sort() {
        if (checkMemoryBlock()
                && checkPartition()) {
            return true;
        }

        //Perform sort
        binaryKeyValueSortedStorage.slotIterator(direction);
        return false;
    }

    private boolean checkMemoryBlock() {
        if (partition == null) {
            return true;
        }

        if (memoryBlockId > partition.getMemoryBlockChain().size() - 1) {
            return true;
        }

        for (int blockIdx = memoryBlockId;
             blockIdx < partition.getMemoryBlockChain().size();
             blockIdx++) {
            MemoryBlock memoryBlock = partition.getMemoryBlockChain().getElement(blockIdx);
            storageHeader.setMemoryBlock(memoryBlock);

            if (storageHeader.getBaseStorageAddress() != MemoryUtil.NULL_VALUE) {
                memoryBlockId = blockIdx + 1;
                binaryKeyValueSortedStorage.setMemoryBlock(memoryBlock);
                binaryKeyValueSortedStorage.gotoAddress(storageHeader.getBaseStorageAddress());

                if (binaryKeyValueSortedStorage.count() > 0) {
                    resultChain.addElement(memoryBlock);
                }

                return false;
            }
        }

        memoryBlockId = partition.getMemoryBlockChain().size();
        return true;
    }

    private boolean checkPartition() {
        if (partitionId > partitions.length - 1) {
            partitionId = 0;
            return true;
        }

        memoryBlockId = 0;
        partition = partitions[partitionId];
        binaryKeyValueSortedStorage = partition.getKeyValueStorage();
        partitionId++;
        return checkMemoryBlock();
    }

    private void reset() {
        partitionId = 0;
        partition = null;
        memoryBlockId = 0;
        resultChain.clear();
    }
}
