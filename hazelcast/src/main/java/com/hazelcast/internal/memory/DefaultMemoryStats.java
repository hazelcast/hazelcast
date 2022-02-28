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

import com.hazelcast.memory.MemorySize;

import static com.hazelcast.internal.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.internal.memory.MemoryStatsSupport.totalPhysicalMemory;

/**
 * Default implementation of MemoryStats.
 */
public class DefaultMemoryStats implements MemoryStats {

    private final Runtime runtime = Runtime.getRuntime();
    private final DefaultGarbageCollectorStats gcStats = new DefaultGarbageCollectorStats();

    @Override
    public final long getTotalPhysical() {
        return totalPhysicalMemory();
    }

    @Override
    public final long getFreePhysical() {
        return freePhysicalMemory();
    }

    @Override
    public final long getMaxHeap() {
        return runtime.maxMemory();
    }

    @Override
    public final long getCommittedHeap() {
        return runtime.totalMemory();
    }

    @Override
    public final long getUsedHeap() {
        return runtime.totalMemory() - runtime.freeMemory();
    }

    @Override
    public final long getFreeHeap() {
        return runtime.freeMemory();
    }

    @Override
    public long getMaxNative() {
        return 0;
    }

    @Override
    public long getCommittedNative() {
        return 0;
    }

    @Override
    public long getUsedNative() {
        return 0;
    }

    @Override
    public long getFreeNative() {
        return 0;
    }

    @Override
    public long getMaxMetadata() {
        return 0;
    }

    @Override
    public long getUsedMetadata() {
        return 0;
    }

    @Override
    public GarbageCollectorStats getGCStats() {
        GCStatsSupport.fill(gcStats);
        return gcStats;
    }

    @Override
    public String toString() {
        return "MemoryStats{"
                + "Total Physical: " + MemorySize.toPrettyString(getTotalPhysical())
                + ", Free Physical: " + MemorySize.toPrettyString(getFreePhysical())
                + ", Max Heap: " + MemorySize.toPrettyString(getMaxHeap())
                + ", Committed Heap: " + MemorySize.toPrettyString(getCommittedHeap())
                + ", Used Heap: " + MemorySize.toPrettyString(getUsedHeap())
                + ", Free Heap: " + MemorySize.toPrettyString(getFreeHeap())
                + ", " + getGCStats()
                + '}';
    }
}
