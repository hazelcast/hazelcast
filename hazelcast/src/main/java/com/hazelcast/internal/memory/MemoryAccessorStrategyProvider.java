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

import com.hazelcast.internal.memory.impl.AlignmentUtil;
import com.hazelcast.internal.memory.impl.StandardMemoryAccessor;
import com.hazelcast.internal.memory.impl.AlignmentAwareMemoryAccessor;
import com.hazelcast.internal.memory.impl.AlignmentMemoryAccessStrategy;
import com.hazelcast.internal.memory.impl.ByteBufferMemoryAccessStrategy;
import com.hazelcast.internal.memory.impl.UnsafeBasedMemoryAccessStrategy;

import java.util.EnumMap;
import java.util.Map;

public final class MemoryAccessorStrategyProvider {
    private static final Map<MemoryAccessStrategyType, MemoryAccessStrategy> MEMORY_ACCESSOR_STRATEGY_MAP
            = new EnumMap<MemoryAccessStrategyType, MemoryAccessStrategy>(MemoryAccessStrategyType.class);

    static {
        final boolean unalignedAccessAllowed = AlignmentUtil.isUnalignedAccessAllowed();

        if (StandardMemoryAccessor.isAvailable()) {
            MemoryAccessStrategy unsafeStrategy = new UnsafeBasedMemoryAccessStrategy();
            MEMORY_ACCESSOR_STRATEGY_MAP.put(MemoryAccessStrategyType.STANDARD, unsafeStrategy);

            if (unalignedAccessAllowed) {
                MEMORY_ACCESSOR_STRATEGY_MAP.put(MemoryAccessStrategyType.PLATFORM_AWARE, unsafeStrategy);
            }
        }

        if (AlignmentAwareMemoryAccessor.isAvailable()) {
            MemoryAccessStrategy alignedStrategy = new AlignmentMemoryAccessStrategy();
            MEMORY_ACCESSOR_STRATEGY_MAP.put(MemoryAccessStrategyType.STANDARD_ALIGNMENT, alignedStrategy);

            if (!unalignedAccessAllowed) {
                MEMORY_ACCESSOR_STRATEGY_MAP.put(MemoryAccessStrategyType.PLATFORM_AWARE, alignedStrategy);
            }
        }


        MemoryAccessStrategy byteStrategy = new ByteBufferMemoryAccessStrategy();
        MEMORY_ACCESSOR_STRATEGY_MAP.put(MemoryAccessStrategyType.HEAP_BYTE_ARRAY, byteStrategy);
    }

    private MemoryAccessorStrategyProvider() {

    }

    /**
     * Returns the {@link MemoryAccessStrategy} instance appropriate to the given {@link MemoryAccessorType}.
     */
    @SuppressWarnings("unchecked")
    public static <R> MemoryAccessStrategy<R> getMemoryAccessStrategy(MemoryAccessStrategyType memoryAccessorType) {
        return (MemoryAccessStrategy<R>) MEMORY_ACCESSOR_STRATEGY_MAP.get(memoryAccessorType);
    }
}
