/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.memory.GlobalMemoryAccessorType.ALIGNMENT_AWARE;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorType.PLATFORM_AWARE;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorType.STANDARD;

/**
 * Provides {@link GlobalMemoryAccessor} implementations by their {@link GlobalMemoryAccessorType}.
 */
public final class GlobalMemoryAccessorRegistry {

    /**
     * A {@link GlobalMemoryAccessor} that allows both aligned and unaligned native memory access.
     */
    public static final GlobalMemoryAccessor MEM;

    /**
     * If this constant is {@code true}, then {@link #MEM} refers to a usable {@code MemoryAccessor}
     * instance.
     */
    public static final boolean MEM_AVAILABLE;

    /**
     * Like {@link #MEM}, but an instance specialized for aligned memory access. Requesting
     * unaligned memory access from this instance will result in low-level JVM crash on platforms
     * which do not support it.
     */
    public static final GlobalMemoryAccessor AMEM;

    /**
     * If this constant is {@code true}, then {@link #AMEM} refers to a usable {@code MemoryAccessor}
     * instance.
     */
    public static final boolean AMEM_AVAILABLE;

    private static final Map<GlobalMemoryAccessorType, GlobalMemoryAccessor> STORAGE =
            new EnumMap<GlobalMemoryAccessorType, GlobalMemoryAccessor>(GlobalMemoryAccessorType.class);

    static {
        final boolean unalignedAccessAllowed = AlignmentUtil.isUnalignedAccessAllowed();

        if (StandardMemoryAccessor.isAvailable()) {
            STORAGE.put(STANDARD, StandardMemoryAccessor.INSTANCE);
            if (unalignedAccessAllowed) {
                STORAGE.put(PLATFORM_AWARE, StandardMemoryAccessor.INSTANCE);
            }
        }
        if (AlignmentAwareMemoryAccessor.isAvailable()) {
            STORAGE.put(ALIGNMENT_AWARE, AlignmentAwareMemoryAccessor.INSTANCE);
            if (!STORAGE.containsKey(PLATFORM_AWARE)) {
                STORAGE.put(PLATFORM_AWARE, AlignmentAwareMemoryAccessor.INSTANCE);
            }
        }
        MEM = getDefaultGlobalMemoryAccessor();
        MEM_AVAILABLE = MEM != null;
        AMEM = getGlobalMemoryAccessor(STANDARD);
        AMEM_AVAILABLE = AMEM != null;
    }

    private GlobalMemoryAccessorRegistry() {
    }

    /**
     * Returns the {@link MemoryAccessor} instance appropriate to the given {@link GlobalMemoryAccessorType}.
     */
    public static GlobalMemoryAccessor getGlobalMemoryAccessor(GlobalMemoryAccessorType type) {
        return STORAGE.get(type);
    }

    /**
     * Returns the default {@link GlobalMemoryAccessor} instance.
     */
    public static GlobalMemoryAccessor getDefaultGlobalMemoryAccessor() {
        return STORAGE.get(PLATFORM_AWARE);
    }
}
