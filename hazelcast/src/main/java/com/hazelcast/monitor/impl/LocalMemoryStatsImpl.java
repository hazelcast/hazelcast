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
        setFreePhysical(memoryStats.getAvailable());
        setTotalNative(memoryStats.getNativeMemoryStats().getTotal());
        setCommittedNativeMemory(memoryStats.getNativeMemoryStats().getCommitted());
        setUsedNativeMemory(memoryStats.getNativeMemoryStats().getUsed());
        setFreeNativeMemory(memoryStats.getNativeMemoryStats().getAvailable());
        setTotalHeap(memoryStats.getHeapMemoryStats().getTotal());
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
    public long getAvailable() {
        return freePhysical;
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

    public void setTotalNative(long totalNative) {
        nativeMemoryStats.setTotal(totalNative);
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

    public void setTotalHeap(long totalHeap) {
        heapMemoryStats.setTotal(totalHeap);
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
        root.add("totalNative", nativeMemoryStats.getTotal());
        root.add("committedNative", nativeMemoryStats.getCommitted());
        root.add("usedNative", nativeMemoryStats.getUsed());
        root.add("freeNative", nativeMemoryStats.getAvailable());
        root.add("totalHeap", heapMemoryStats.getTotal());
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
        nativeMemoryStats.setTotal(getLong(json, "totalNative", -1L));
        nativeMemoryStats.setCommitted(getLong(json, "committedNative", -1L));
        nativeMemoryStats.setUsed(getLong(json, "usedNative", -1L));
        nativeMemoryStats.setFree(getLong(json, "freeNative", -1L));
        heapMemoryStats.setTotal(getLong(json, "totalHeap", -1L));
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
                + ", totalNative=" + nativeMemoryStats.getTotal()
                + ", committedNative=" + nativeMemoryStats.getCommitted()
                + ", usedNative=" + nativeMemoryStats.getUsed()
                + ", totalHeap=" + heapMemoryStats.getTotal()
                + ", committedHeap=" + heapMemoryStats.getCommitted()
                + ", usedHeap=" + heapMemoryStats.getUsed()
                + ", gcStats=" + gcStats
                + '}';
    }

    private static class HeapMemoryStats extends BaseMemoryStats {
        @Override
        public long getAvailable() {
            return getTotal() - getUsed();
        }
    }

    private static class BaseMemoryStats implements MemoryStats {
        private long total;

        private long committed;

        private long used;

        private long free;

        @Override
        public long getTotal() {
            return total;
        }

        @Override
        public long getAvailable() {
            return free;
        }

        @Override
        public long getCommitted() {
            return committed;
        }

        @Override
        public long getUsed() {
            return used;
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
