/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Disposable;

/**
 * Contract to allocate and access memory in an abstract address space, which is not necessarily the
 * underlying CPU's native address space. Memory allocated from the {@link MemoryAllocator} must be
 * accessed through the {@link MemoryAccessor}.
 */
public interface MemoryManager extends Disposable {

    /**
     * @return the associated {@link MemoryAllocator}
     */
    MemoryAllocator getAllocator();

    /**
     * @return the associated {@link MemoryAccessor}
     */
    MemoryAccessor getAccessor();
}
