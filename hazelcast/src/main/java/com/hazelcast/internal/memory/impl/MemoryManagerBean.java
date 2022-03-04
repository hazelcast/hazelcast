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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;

/**
 * Simple implementation of {@link MemoryManager} which aggregates the
 * {@link MemoryAllocator} and {@link MemoryAccessor} supplied at construction time.
 */
public class MemoryManagerBean implements MemoryManager {

    private final MemoryAllocator allocator;
    private final MemoryAccessor accessor;

    public MemoryManagerBean(MemoryAllocator allocator, MemoryAccessor accessor) {
        this.allocator = allocator;
        this.accessor = accessor;
    }

    @Override
    public MemoryAllocator getAllocator() {
        return allocator;
    }

    @Override
    public MemoryAccessor getAccessor() {
        return accessor;
    }

    @Override
    public void dispose() {
        allocator.dispose();
    }
}
