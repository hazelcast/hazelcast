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

package com.hazelcast.jet.memory.impl.memory.impl.management.memoryblock;

import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.memory.MemoryType;
import com.hazelcast.jet.memory.spi.memory.MemoryPool;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;

public class MemoryBlockPool {
    private final long blockSize;
    private final boolean useBigEndian;
    private final MemoryPool memoryPool;
    private final MemoryType memoryType;
    private final Queue<MemoryBlock> segmentQueue;

    public MemoryBlockPool(
            long blockSize,
            boolean useBigEndian,
            MemoryPool memoryPool
    ) {
        this.blockSize = blockSize;
        this.memoryPool = memoryPool;
        this.useBigEndian = useBigEndian;
        this.memoryType = memoryPool.getMemoryType();
        this.segmentQueue = new ConcurrentLinkedQueue<>();
    }

    public MemoryBlock getNextMemoryBlock(boolean useAux) {
        MemoryBlock memoryBlock = segmentQueue.poll();

        if (memoryBlock == null) {
            if (memoryPool.reserve(blockSize)) {
                memoryBlock =
                        memoryType == MemoryType.HEAP
                                ?
                                new HeapMemoryBlock(blockSize, useAux, useBigEndian)
                                :
                                new NativeMemoryBlock(blockSize, useAux, useBigEndian);
            }
        }

        return memoryBlock;
    }

    public void releaseMemoryBlock(MemoryBlock memoryBlock) {
        segmentQueue.offer(memoryBlock);
    }
}
