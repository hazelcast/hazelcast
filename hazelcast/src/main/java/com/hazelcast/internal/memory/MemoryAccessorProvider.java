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

import com.hazelcast.internal.memory.impl.AlignmentAwareMemoryAccessor;
import com.hazelcast.internal.memory.impl.AlignmentUtil;
import com.hazelcast.internal.memory.impl.StandardMemoryAccessor;

import java.util.EnumMap;
import java.util.Map;

/**
 * Provides {@link MemoryAccessor} implementations by their {@link MemoryAccessorType}.
 */
public final class MemoryAccessorProvider {

    private static final Map<MemoryAccessorType, MemoryAccessor> MEMORY_ACCESSOR_MAP
            = new EnumMap<MemoryAccessorType, MemoryAccessor>(MemoryAccessorType.class);

    static {
        final boolean unalignedAccessAllowed = AlignmentUtil.isUnalignedAccessAllowed();

        if (StandardMemoryAccessor.isAvailable()) {
            StandardMemoryAccessor standardMemoryAccessor = new StandardMemoryAccessor();
            MEMORY_ACCESSOR_MAP.put(MemoryAccessorType.STANDARD, standardMemoryAccessor);
            if (unalignedAccessAllowed) {
                MEMORY_ACCESSOR_MAP.put(MemoryAccessorType.PLATFORM_AWARE, standardMemoryAccessor);
            }
        }

        if (AlignmentAwareMemoryAccessor.isAvailable()) {
            AlignmentAwareMemoryAccessor alignmentAwareMemoryAccessor = new AlignmentAwareMemoryAccessor();
            MEMORY_ACCESSOR_MAP.put(MemoryAccessorType.ALIGNMENT_AWARE, alignmentAwareMemoryAccessor);
            if (!unalignedAccessAllowed) {
                MEMORY_ACCESSOR_MAP.put(MemoryAccessorType.PLATFORM_AWARE, alignmentAwareMemoryAccessor);
            }
        }
    }

    private MemoryAccessorProvider() {
    }

    /**
     * Returns the {@link MemoryAccessor} instance appropriate to the given {@link MemoryAccessorType}.
     */
    public static MemoryAccessor getMemoryAccessor(MemoryAccessorType memoryAccessorType) {
        return MEMORY_ACCESSOR_MAP.get(memoryAccessorType);
    }

    /**
     * Returns the default {@link MemoryAccessor} instance.
     */
    public static MemoryAccessor getDefaultMemoryAccessor() {
        return MEMORY_ACCESSOR_MAP.get(MemoryAccessorType.PLATFORM_AWARE);
    }
}
