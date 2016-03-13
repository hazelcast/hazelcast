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

package com.hazelcast.spi.memory;

import com.hazelcast.spi.impl.memory.BigEndianMemoryAccessor;
import com.hazelcast.spi.impl.memory.LittleEndianMemoryAccessor;
import com.hazelcast.spi.impl.memory.AlignmentUtil;

import static com.hazelcast.spi.memory.GlobalMemoryAccessorRegistry.MEM;

/**
 * Accesses global memory (Java heap and native memory) using a specific, not necessarily native endianness.
 * Native-endian access will be delegated to the {@link GlobalMemoryAccessorRegistry#MEM} instance to provide optimum
 * performance.
 */
public interface EndianMemoryAccessor extends MemoryAccessor, HeapMemoryAccessor {

    /**
     * The big-endian accessor instance.
     */
    EndianMemoryAccessor MEM_BE = AlignmentUtil.IS_PLATFORM_BIG_ENDIAN ? MEM : new BigEndianMemoryAccessor();

    /**
     * The little-endian accessor instance.
     */
    EndianMemoryAccessor MEM_LE = AlignmentUtil.IS_PLATFORM_BIG_ENDIAN ? new LittleEndianMemoryAccessor() : MEM;

    /**
     * @return true if this accessor is big-endian; false if little-endian
     */
    boolean isBigEndian();
}
