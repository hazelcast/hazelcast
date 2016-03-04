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

import static com.hazelcast.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.memory.MemoryStatsSupport.totalPhysicalMemory;

/**
 * Default implementation of MemoryStats.
 */
public class DefaultMemoryStats implements JvmMemoryStats {
    private final DefaultGarbageCollectorStats gcStats = new DefaultGarbageCollectorStats();

    private final MemoryStats heapMemoryStats = new HeapMemoryStats();

    private final MemoryStats nativeMemoryStats;

    public DefaultMemoryStats() {
        this.nativeMemoryStats = new DummyMemoryStats();
    }

    protected DefaultMemoryStats(MemoryStats nativeMemoryStats) {
        this.nativeMemoryStats = nativeMemoryStats;
    }

    @Override
    public MemoryStats getHeapMemoryStats() {
        return heapMemoryStats;
    }

    @Override
    public MemoryStats getNativeMemoryStats() {
        return nativeMemoryStats;
    }

    @Override
    public GarbageCollectorStats getGCStats() {
        GCStatsSupport.fill(gcStats);
        return gcStats;
    }

    @Override
    public long getTotal() {
        return totalPhysicalMemory();
    }

    @Override
    public long getFree() {
        return freePhysicalMemory();
    }

    @Override
    public long getMax() {
        return getTotal();
    }

    @Override
    public long getCommitted() {
        return getFree();
    }

    public long getUsed() {
        return getTotal() - getFree();
    }

    @Override
    public String toString() {
        return "MemoryStats{"
                + "Total Physical: " + MemorySize.toPrettyString(totalPhysicalMemory())
                + ", Free Physical: " + MemorySize.toPrettyString(getFree())
                + ", Max Heap: " + MemorySize.toPrettyString(getHeapMemoryStats().getMax())
                + ", Committed Heap: " + MemorySize.toPrettyString(getHeapMemoryStats().getCommitted())
                + ", Used Heap: " + MemorySize.toPrettyString(getHeapMemoryStats().getUsed())
                + ", Free Heap: " + MemorySize.toPrettyString(getHeapMemoryStats().getFree())
                + ", " + getGCStats()
                + '}';
    }
}
