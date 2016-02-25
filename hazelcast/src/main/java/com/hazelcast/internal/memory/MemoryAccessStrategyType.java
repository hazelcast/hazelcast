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

package com.hazelcast.internal.memory;

/**
 * Provides different types to access data from external resources (objects , byte arrays)
 */
public enum MemoryAccessStrategyType {
    /**
     * Represents the UNSAFE-based {@link MemoryAccessor}.
     *
     * @see com.hazelcast.internal.memory.impl.UnsafeBasedMemoryAccessStrategy
     */
    STANDARD,

    /**
     * Represents the UNSAFE-based {@link MemoryAccessor} with memory alignment.
     *
     * @see com.hazelcast.internal.memory.impl.AlignmentMemoryAccessStrategy
     */
    STANDARD_ALIGNMENT,

    /**
     * Represents the heap-byte {@link MemoryAccessor} which  data in specified heap-byteBuffer (byte[] buffer).
     *
     * @see com.hazelcast.internal.memory.impl.ByteBufferMemoryAccessStrategy
     */
    HEAP_BYTE_ARRAY,

    /**
     * Represents a {@link MemoryAccessor} that checks the underlying platform and behaves as either
     * {@link #STANDARD_ALIGNMENT} or {@link #STANDARD}, as appropriate to the platform's architecture.
     * <p>
     * If the underlying platform supports unaligned memory access,
     * it behaves as the standard {@link MemoryAccessor} because there's no need for additional checks.
     * Otherwise, it behaves as the alignment-aware {@link MemoryAccessor}.
     */
    PLATFORM_AWARE;
}
