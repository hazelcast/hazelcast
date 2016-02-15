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
     * Represents standard {@link MemoryAccessor} directly uses {@link sun.misc.Unsafe} for accessing to memory.
     *
     * @see com.hazelcast.internal.memory.impl.StandardMemoryAccessor
     */
    STANDARD,

    /**
     * Represents aligned {@link MemoryAccessor} checks and handles unaligned memory accesses if possible
     * by using {@link sun.misc.Unsafe} for accessing to memory.
     *
     * @see com.hazelcast.internal.memory.impl.AlignmentAwareMemoryAccessor
     */
    ALIGNMENT_AWARE,

    /**
     * Represents {@link MemoryAccessor} that checks underlying platform and behaves regarding to its architecture
     * as aligned or standard.
     *
     * If the underlying platform supports unaligned memory accesses,
     * it behaves as standard {@link MemoryAccessor} because no need to additional checks.
     * Otherwise, it behaves as alignment-aware {@link MemoryAccessor}.
     *
     * @see com.hazelcast.internal.memory.impl.PlatformAwareMemoryAccessor
     */
    PLATFORM_AWARE;

}
