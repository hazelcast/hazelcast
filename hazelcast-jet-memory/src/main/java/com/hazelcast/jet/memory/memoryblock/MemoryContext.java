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

package com.hazelcast.jet.memory.memoryblock;

import com.hazelcast.jet.memory.JetMemoryException;

/**
 * Manages heap and native memory pools.
 */
public class MemoryContext {
    private final MemoryBlockPool heapMemoryBlockPool;
    private final MemoryBlockPool nativeMemoryBlockPool;

    public MemoryContext(MemoryPool heapMemoryPool, MemoryPool nativeMemoryPool, long blockSize, boolean useBigEndian) {
        this.heapMemoryBlockPool = new MemoryBlockPool(blockSize, useBigEndian, heapMemoryPool);
        this.nativeMemoryBlockPool = new MemoryBlockPool(blockSize, useBigEndian, nativeMemoryPool);
    }

    public MemoryBlockPool getMemoryBlockPool(MemoryType type) {
        switch (type) {
            case HEAP: return heapMemoryBlockPool;
            case NATIVE: return nativeMemoryBlockPool;
            default: throw new JetMemoryException("Unknown memoryType " + type);
        }
    }
}
