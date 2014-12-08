/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
 * <p/>
 * Shows total physical/free OS memory, max/committed/used/free heap memory
 * and max/committed/used/free native memory.
 *
 * @see com.hazelcast.memory.GarbageCollectorStats
 */
public interface MemoryStats {

    /**
     * Returns total physical memory available in OS.
     *
     * @return total physical memory in bytes.
     */
    long getTotalPhysical();

    /**
     * Returns free physical memory available in OS.
     * <p/>
     * Some operating systems have cached memory regions in which they store hot cached resources like files.
     * Cached memory is used for allocations when required. So although this cached region is
     * assumed as free, operating systems generally report this region as used not free.
     *
     * @return free physical memory in bytes.
     */
    long getFreePhysical();

    /**
     * Returns the maximum amount of memory that the JVM will attempt to use in bytes.
     *
     * @return the maximum amount of memory in bytes.
     *
     * @see Runtime#maxMemory()
     */
    long getMaxHeap();

    /**
     * Returns the amount of memory in bytes that is committed for
     * the Java virtual machine to use.
     *
     * @return the amount of committed memory in bytes.
     *
     * @see Runtime#totalMemory()
     * @see java.lang.management.MemoryUsage#getCommitted()
     */
    long getCommittedHeap();

    /**
     * Returns the amount of used memory in the JVM in bytes.
     *
     * @return the amount of used memory in bytes
     *
     * @see java.lang.management.MemoryUsage#getUsed()
     */
    long getUsedHeap();

    /**
     * Returns the amount of free memory in the JVM in bytes.
     *
     * @return the amount of free memory in bytes
     *
     * @see Runtime#freeMemory()
     */
    long getFreeHeap();

    /**
     * Returns the maximum amount of native memory that current HazelcastInstance
     * will attempt to use in bytes.
     *
     * @return the maximum amount of native memory in bytes.
     */
    long getMaxNativeMemory();

    /**
     * Returns the amount of native memory in bytes that is committed for
     * current HazelcastInstance to use.
     *
     * @return the amount of committed native memory in bytes.
     */
    long getCommittedNativeMemory();

    /**
     * Returns the amount of used native memory in current HazelcastInstance in bytes.
     *
     * @return the amount of used native memory in bytes
     */
    long getUsedNativeMemory();

    /**
     * Returns the amount of free native memory in current HazelcastInstance in bytes.
     *
     * @return the amount of free native memory in bytes
     */
    long getFreeNativeMemory();

    /**
     * Returns the garbage collector statistics for the JVM
     * @return GC statistics
     */
    GarbageCollectorStats getGCStats();

}
