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

package com.hazelcast.jet.memory.api.memory.management;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.jet.memory.spi.memory.MemoryType;

/**
 * Represents memory block abstraction;
 *
 * @param <Resource> - type if base resource (address or byte-array);
 */
public interface MemoryBlock<Resource>
        extends MemoryManager, MemoryAccessor, MemoryAllocator {
    long TOP_OFFSET = 1L;

    void reset();

    long getUsedBytes();

    Resource getResource();

    long toAddress(long pos);

    long getAvailableBytes();

    long getTotalBytes();

    void reset(boolean useAux);

    MemoryType getMemoryType();

    void copyFromMemoryBlock(MemoryBlock sourceMemoryBlock,
                             long sourceOffset,
                             long dstOffset,
                             long size);

    /**
     * Reset current memory block with aux=true mode;
     *
     * @return Aux-specified memory manager;
     */
    MemoryManager getAuxMemoryManager();
}
