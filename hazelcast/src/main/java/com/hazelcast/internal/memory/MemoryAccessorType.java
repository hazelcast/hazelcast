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
 * Types of the {@link MemoryAccessor} implementations.
 */
public enum MemoryAccessorType {

    /**
     * Represents the standard {@link MemoryAccessor} which directly uses
     * {@link sun.misc.Unsafe} to access memory.
     *
     * @see com.hazelcast.internal.memory.impl.StandardMemoryAccessor
     */
    STANDARD,

    /**
     * Represents the aligned {@link MemoryAccessor}, which checks for and handles unaligned memory access
     * by splitting a larger-size memory operation into several smaller-size ones
     * (which have finer-grained alignment requirements).
     *
     * @see com.hazelcast.internal.memory.impl.AlignmentAwareMemoryAccessor
     */
    ALIGNMENT_AWARE,

    /**
     * Represents a {@link MemoryAccessor} that checks the underlying platform and behaves as either
     * {@link #ALIGNMENT_AWARE} or {@link #STANDARD}, as appropriate to the platform's architecture.
     * <p>
     * If the underlying platform supports unaligned memory access,
     * it behaves as the standard {@link MemoryAccessor} because there's no need for additional checks.
     * Otherwise, it behaves as the alignment-aware {@link MemoryAccessor}.
     *
     * @see com.hazelcast.internal.memory.impl.PlatformAwareMemoryAccessor
     */
    PLATFORM_AWARE;

}
