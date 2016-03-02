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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.memory.JvmMemoryStats;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.monitor.LocalGCStats;
import com.hazelcast.monitor.LocalMemoryStats;

import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getObject;

public class LocalMemoryStatsImpl implements LocalMemoryStats {

    private long creationTime;

    private long totalPhysical;

    private long freePhysical;

    private final HeapMemoryStats heapMemoryStats = new HeapMemoryStats();

    private final BaseMemoryStats nativeMemoryStats = new BaseMemoryStats();

    private LocalGCStats gcStats;

    public LocalMemoryStatsImpl() {
    }

    public LocalMemoryStatsImpl(JvmMemoryStats memoryStats) {
        setTotalPhysical(memoryStats.getTotal());
        setFreePhysical(memoryStats.getFree());
        setMaxNativeMemory(memoryStats.getNativeMemoryStats().getMax());
        setCommittedNativeMemory(memoryStats.getNativeMemoryStats().getCommitted());
        setUsedNativeMemory(memoryStats.getNativeMemoryStats().getUsed());
        setFreeNativeMemory(memoryStats.getNativeMemoryStats().getFree());
        setMaxHeap(memoryStats.getHeapMemoryStats().getMax());
        setCommittedHeap(memoryStats.getHeapMemoryStats().getCommitted());
        setUsedHeap(memoryStats.getHeapMemoryStats().getUsed());
        setGcStats(new LocalGCStatsImpl(memoryStats.getGCStats()));
    }

    @Override
    public long getTotal() {
        return totalPhysical;
    }

    public void setTotalPhysical(long totalPhysical) {
        this.totalPhysical = totalPhysical;
    }

    @Override
    public long getFree() {
        return freePhysical;
    }

    @Override
    public long getMax() {
        return totalPhysical;
    }

    @Override
    public long getCommitted() {
        return freePhysical;
    }

    @Override
    public long getUsed() {
        return totalPhysical - freePhysical;
    }

    public void setFreePhysical(long freePhysical) {
        this.freePhysical = freePhysical;
    }

    public void setMaxNativeMemory(long maxNativeMemory) {
        nativeMemoryStats.setMax(maxNativeMemory);
    }

    public void setUsedNativeMemory(long used) {
        nativeMemoryStats.setUsed(used);
    }

    public void setFreeNativeMemory(long freeNativeMemory) {
        nativeMemoryStats.setFree(freeNativeMemory);
    }

    public void setCommittedNativeMemory(long committed) {
        nativeMemoryStats.setCommitted(committed);
    }

    public void setMaxHeap(long maxHeap) {
        heapMemoryStats.setMax(maxHeap);
    }

    public void setCommittedHeap(long committedHeap) {
        heapMemoryStats.setCommitted(committedHeap);
    }

    public void setUsedHeap(long usedHeap) {
        heapMemoryStats.setUsed(usedHeap);
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
    public LocalGCStats getGCStats() {
        return gcStats;
    }

    public void setGcStats(LocalGCStats gcStats) {
        this.gcStats = gcStats;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("totalPhysical", totalPhysical);
        root.add("freePhysical", freePhysical);
        root.add("maxNativeMemory", nativeMemoryStats.getMax());
        root.add("committedNativeMemory", nativeMemoryStats.getCommitted());
        root.add("usedNativeMemory", nativeMemoryStats.getUsed());
        root.add("freeNativeMemory", nativeMemoryStats.getFree());
        root.add("maxHeap", heapMemoryStats.getMax());
        root.add("committedHeap", heapMemoryStats.getCommitted());
        root.add("usedHeap", heapMemoryStats.getUsed());
        if (gcStats == null) {
            gcStats = new LocalGCStatsImpl();
        }
        root.add("gcStats", gcStats.toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        totalPhysical = getLong(json, "totalPhysical", -1L);
        freePhysical = getLong(json, "freePhysical", -1L);
        nativeMemoryStats.setMax(getLong(json, "maxNativeMemory", -1L));
        nativeMemoryStats.setCommitted(getLong(json, "committedNativeMemory", -1L));
        nativeMemoryStats.setUsed(getLong(json, "usedNativeMemory", -1L));
        nativeMemoryStats.setFree(getLong(json, "freeNativeMemory", -1L));
        heapMemoryStats.setMax(getLong(json, "maxHeap", -1L));
        heapMemoryStats.setCommitted(getLong(json, "committedHeap", -1L));
        heapMemoryStats.setUsed(getLong(json, "usedHeap", -1L));
        gcStats = new LocalGCStatsImpl();

        if (json.get("gcStats") != null) {
            gcStats.fromJson(getObject(json, "gcStats"));
        }
    }

    @Override
    public String toString() {
        return "LocalMemoryStats{"
                + "totalPhysical=" + totalPhysical
                + ", freePhysical=" + freePhysical
                + ", maxNativeMemory=" + nativeMemoryStats.getMax()
                + ", committedNativeMemory=" + nativeMemoryStats.getCommitted()
                + ", usedNativeMemory=" + nativeMemoryStats.getUsed()
                + ", maxHeap=" + heapMemoryStats.getMax()
                + ", committedHeap=" + heapMemoryStats.getCommitted()
                + ", usedHeap=" + heapMemoryStats.getUsed()
                + ", gcStats=" + gcStats
                + '}';
    }

    private class HeapMemoryStats extends BaseMemoryStats {
        @Override
        public long getFree() {
            return getMax() - getUsed();
        }
    }

    private class BaseMemoryStats implements MemoryStats {
        private long max;

        private long committed;

        private long used;

        private long total;

        private long free;

        @Override
        public long getTotal() {
            return total;
        }

        @Override
        public long getFree() {
            return free;
        }

        @Override
        public long getMax() {
            return max;
        }

        @Override
        public long getCommitted() {
            return committed;
        }

        @Override
        public long getUsed() {
            return used;
        }

        public void setMax(long max) {
            this.max = max;
        }

        public void setCommitted(long committed) {
            this.committed = committed;
        }

        public void setUsed(long used) {
            this.used = used;
        }

        public void setTotal(long total) {
            this.total = total;
        }

        public void setFree(long free) {
            this.free = free;
        }
    }
}
