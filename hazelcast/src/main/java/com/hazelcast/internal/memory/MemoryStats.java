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

import com.hazelcast.internal.metrics.Probe;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_COMMITTED_HEAP;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_COMMITTED_NATIVE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_FREE_HEAP;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_FREE_NATIVE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_FREE_PHYSICAL;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_MAX_HEAP;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_MAX_METADATA;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_MAX_NATIVE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_TOTAL_PHYSICAL;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_USED_HEAP;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_USED_METADATA;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MEMORY_METRIC_USED_NATIVE;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;

/**
 * Memory statistics for the JVM which current HazelcastInstance belongs to.
 * <p>
 * Shows total physical/free OS memory, max/committed/used/free heap memory
 * and max/committed/used/free native memory.
 *
 * @see com.hazelcast.internal.memory.GarbageCollectorStats
 */
public interface MemoryStats {

    /**
     * Returns total physical memory available in OS.
     *
     * @return total physical memory in bytes.
     */
    @Probe(name = MEMORY_METRIC_TOTAL_PHYSICAL, level = MANDATORY, unit = BYTES)
    long getTotalPhysical();

    /**
     * Returns free physical memory available in OS.
     * <p>
     * Some operating systems have cached memory regions in which they store hot cached resources like files.
     * Cached memory is used for allocations when required. So although this cached region is
     * assumed as free, operating systems generally report this region as used not free.
     *
     * @return free physical memory in bytes.
     */
    @Probe(name = MEMORY_METRIC_FREE_PHYSICAL, level = MANDATORY, unit = BYTES)
    long getFreePhysical();

    /**
     * Returns the maximum amount of memory that the JVM will attempt to use in bytes.
     *
     * @return the maximum amount of memory in bytes.
     * @see Runtime#maxMemory()
     */
    @Probe(name = MEMORY_METRIC_MAX_HEAP, level = MANDATORY, unit = BYTES)
    long getMaxHeap();

    /**
     * Returns the amount of memory in bytes that is committed for
     * the Java virtual machine to use.
     *
     * @return the amount of committed memory in bytes.
     * @see Runtime#totalMemory()
     * @see java.lang.management.MemoryUsage#getCommitted()
     */
    @Probe(name = MEMORY_METRIC_COMMITTED_HEAP, level = MANDATORY, unit = BYTES)
    long getCommittedHeap();

    /**
     * Returns the amount of used memory in the JVM in bytes.
     *
     * @return the amount of used memory in bytes
     * @see java.lang.management.MemoryUsage#getUsed()
     */
    @Probe(name = MEMORY_METRIC_USED_HEAP, level = MANDATORY, unit = BYTES)
    long getUsedHeap();

    /**
     * Returns the amount of free memory in the JVM in bytes.
     *
     * @return the amount of free memory in bytes
     * @see Runtime#freeMemory()
     */
    @Probe(name = MEMORY_METRIC_FREE_HEAP, level = MANDATORY, unit = BYTES)
    long getFreeHeap();

    /**
     * Returns the maximum amount of native memory that current HazelcastInstance
     * will attempt to use in bytes.
     *
     * @return the maximum amount of native memory in bytes.
     */
    @Probe(name = MEMORY_METRIC_MAX_NATIVE, level = MANDATORY, unit = BYTES)
    long getMaxNative();

    /**
     * Returns the amount of native memory in bytes that is committed for
     * current HazelcastInstance to use.
     *
     * @return the amount of committed native memory in bytes.
     */
    @Probe(name = MEMORY_METRIC_COMMITTED_NATIVE, level = MANDATORY, unit = BYTES)
    long getCommittedNative();

    /**
     * Returns the amount of used native memory in current HazelcastInstance in bytes.
     *
     * @return the amount of used native memory in bytes
     */
    @Probe(name = MEMORY_METRIC_USED_NATIVE, level = MANDATORY, unit = BYTES)
    long getUsedNative();

    /**
     * Returns the amount of free native memory in current HazelcastInstance in bytes.
     *
     * @return the amount of free native memory in bytes
     */
    @Probe(name = MEMORY_METRIC_FREE_NATIVE, level = MANDATORY, unit = BYTES)
    long getFreeNative();

    /**
     * Returns the amount of native memory reserved for metadata. This memory
     * is separate and not accounted for by the {@code ...NativeMemory} statistics.
     */
    @Probe(name = MEMORY_METRIC_MAX_METADATA, level = MANDATORY, unit = BYTES)
    long getMaxMetadata();

    /**
     * @return amount of used metadata memory
     */
    @Probe(name = MEMORY_METRIC_USED_METADATA, level = MANDATORY, unit = BYTES)
    long getUsedMetadata();

    /**
     * Returns the garbage collector statistics for the JVM
     *
     * @return GC statistics
     */
    GarbageCollectorStats getGCStats();
}
