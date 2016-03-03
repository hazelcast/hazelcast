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
 * Memory statistics for the JVM which current HazelcastInstance belongs to.
 * <p>
 * Shows total physical/free OS memory, max/committed/used/free heap memory
 * and max/committed/used/free native memory.
 *
 * @see com.hazelcast.memory.GarbageCollectorStats
 */
public interface JVMMemoryStats extends MemoryStats {
    /**
     * @return memory statistics for all heap memory
     */
    MemoryStats getHeapMemoryStats();

    /**
     * @return memory statistics for all native memory
     */
    MemoryStats getNativeMemoryStats();

    /**
     * Returns the garbage collector statistics for the JVM
     *
     * @return GC statistics
     */
    GarbageCollectorStats getGCStats();
}
