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

import com.hazelcast.internal.memory.impl.BigEndianMemoryAccessor;
import com.hazelcast.internal.memory.impl.LittleEndianMemoryAccessor;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.impl.AlignmentUtil.IS_PLATFORM_BIG_ENDIAN;

/**
 * Accesses global memory (Java heap and native memory) using a specific, not necessarily native endianness.
 * Native-endian access will be delegated to the {@link GlobalMemoryAccessorRegistry#MEM} instance to provide optimum
 * performance.
 */
public interface EndianMemoryAccessor extends MemoryAccessor, HeapMemoryAccessor {

    /**
     * The big-endian accessor instance.
     */
    EndianMemoryAccessor MEM_BE = IS_PLATFORM_BIG_ENDIAN ? MEM : new BigEndianMemoryAccessor();

    /**
     * The little-endian accessor instance.
     */
    EndianMemoryAccessor MEM_LE = IS_PLATFORM_BIG_ENDIAN ? new LittleEndianMemoryAccessor() : MEM;

    /**
     * @return true if this accessor is big-endian; false if little-endian
     */
    boolean isBigEndian();
}
