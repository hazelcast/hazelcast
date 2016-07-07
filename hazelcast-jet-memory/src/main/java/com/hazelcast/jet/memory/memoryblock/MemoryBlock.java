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

import com.hazelcast.internal.memory.MemoryManager;

/**
 * Manages a memory block using a simple append-only allocation scheme.
 */
public interface MemoryBlock extends MemoryManager {
    long TOP_OFFSET = 1L;

    void reset();

    long getUsedBytes();

    long toAddress(long pos);

    long getAvailableBytes();

    long getTotalBytes();

    void reset(boolean enableReverseAllocator);

    MemoryType type();

    void copyFrom(MemoryBlock sourceMemoryBlock, long sourceOffset, long dstOffset, long size);

    /**
     * Returns the auxiliary memory manager, which may be this object or its companion
     * "reverse" memory manager which manages the same memory block as this one,
     * but allocates from the top of the block downwards. Which of the two managers is returned
     * depends on the current state of the "enableReverseAllocator" flag, as set by the most
     * recent call of {@link #reset(boolean)}.
     */
    MemoryManager getAuxMemoryManager();
}
