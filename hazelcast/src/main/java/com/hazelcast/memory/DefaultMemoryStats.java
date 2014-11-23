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

import static com.hazelcast.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.memory.MemoryStatsSupport.totalPhysicalMemory;

/**
 * Default implementation of MemoryStats.
 */
public class DefaultMemoryStats implements MemoryStats {

    private final Runtime runtime = Runtime.getRuntime();
    private final DefaultGarbageCollectorStats gcStats = new DefaultGarbageCollectorStats();

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
    public long getMaxNativeMemory() {
        return 0;
    }

    @Override
    public long getCommittedNativeMemory() {
        return 0;
    }

    @Override
    public long getUsedNativeMemory() {
        return 0;
    }

    @Override
    public long getFreeNativeMemory() {
        return 0;
    }

    @Override
    public GarbageCollectorStats getGCStats() {
        GCStatsSupport.fill(gcStats);
        return gcStats;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MemoryStats {");
        sb.append("Total Physical: ").append(MemorySize.toPrettyString(getTotalPhysical()));
        sb.append(", Free Physical: ").append(MemorySize.toPrettyString(getFreePhysical()));
        sb.append(", Max Heap: ").append(MemorySize.toPrettyString(getMaxHeap()));
        sb.append(", Committed Heap: ").append(MemorySize.toPrettyString(getCommittedHeap()));
        sb.append(", Used Heap: ").append(MemorySize.toPrettyString(getUsedHeap()));
        sb.append(", Free Heap: ").append(MemorySize.toPrettyString(getFreeHeap()));
        sb.append(", ");
        sb.append(getGCStats());
        sb.append('}');
        return sb.toString();
    }
}
