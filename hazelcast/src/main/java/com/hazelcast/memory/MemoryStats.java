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

package com.hazelcast.memory;

/**
 * Memory usage statistics for a {@code MemoryManager} instance.
 */
public interface MemoryStats {

    /**
     * Returns total memory available to the associated {@code MemoryAllocator}.
     * Only a fraction of this amount may be usable by the client due to
     * overheads from headers and/or fragmentation.
     */
    long getTotal();

    /**
     * The portion of total memory that is in active use by the associated
     * {@code MemoryAllocator}.
     */
    long getCommitted();

    /**
     * Amount of usable memory allocated from the associated {@code MemoryAllocator}. Less
     * than or equal to committed memory.
     */
    long getUsed();

    /**
     * Amount of usable memory available for allocation from the associated
     * {@code MemoryAllocator}. This may be just an estimate because overheads from
     * headers and fragmentation can reduce the memory available to the client
     * in a non-linear fashion.
     */
    long getAvailable();
}
