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
 * Reflects physical memory reported by the underlying OS. Further {@code MemoryStats} instances can
 * be retrieved for heap and Hazelcast-managed native memory stats.
 *
 * @see com.hazelcast.memory.GarbageCollectorStats
 */
public interface JvmMemoryStats extends MemoryStats {
    /**
     * @return memory statistics for the Java heap
     */
    MemoryStats getHeapMemoryStats();

    /**
     * @return memory statistics for Hazelcast-managed native memory
     */
    MemoryStats getNativeMemoryStats();

    /**
     * @return garbage collector statistics for the JVM
     */
    GarbageCollectorStats getGCStats();
}
