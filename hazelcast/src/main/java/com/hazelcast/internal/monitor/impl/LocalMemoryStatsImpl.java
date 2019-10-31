/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.monitor.LocalGCStats;
import com.hazelcast.internal.monitor.LocalMemoryStats;

import static com.hazelcast.internal.util.JsonUtil.getLong;
import static com.hazelcast.internal.util.JsonUtil.getObject;

public class LocalMemoryStatsImpl implements LocalMemoryStats {

    public static final String JSON_CREATION_TIME = "creationTime";
    public static final String JSON_TOTAL_PHYSICAL = "totalPhysical";
    public static final String JSON_FREE_PHYSICAL = "freePhysical";
    public static final String JSON_MAX_NATIVE_MEMORY = "maxNativeMemory";
    public static final String JSON_COMMITTED_NATIVE_MEMORY = "committedNativeMemory";
    public static final String JSON_USED_NATIVE_MEMORY = "usedNativeMemory";
    public static final String JSON_FREE_NATIVE_MEMORY = "freeNativeMemory";
    public static final String JSON_MAX_HEAP = "maxHeap";
    public static final String JSON_COMMITTED_HEAP = "committedHeap";
    public static final String JSON_USED_HEAP = "usedHeap";
    public static final String JSON_GC_STATS = "gcStats";

    private long creationTime;

    private long totalPhysical;

    private long freePhysical;

    private long maxNativeMemory;

    private long committedNativeMemory;

    private long usedNativeMemory;

    private long freeNativeMemory;

    private long maxMetadata;

    private long usedMetadata;

    private long maxHeap;

    private long committedHeap;

    private long usedHeap;

    private LocalGCStats gcStats;

    public LocalMemoryStatsImpl() {
    }

    public LocalMemoryStatsImpl(MemoryStats memoryStats) {
        setTotalPhysical(memoryStats.getTotalPhysical());
        setFreePhysical(memoryStats.getFreePhysical());
        setMaxNativeMemory(memoryStats.getMaxNative());
        setCommittedNativeMemory(memoryStats.getCommittedNative());
        setUsedNativeMemory(memoryStats.getUsedNative());
        setFreeNativeMemory(memoryStats.getFreeNative());
        setMaxMetadata(memoryStats.getMaxMetadata());
        setUsedMetadata(memoryStats.getUsedMetadata());
        setMaxHeap(memoryStats.getMaxHeap());
        setCommittedHeap(memoryStats.getCommittedHeap());
        setUsedHeap(memoryStats.getUsedHeap());
        setGcStats(new LocalGCStatsImpl(memoryStats.getGCStats()));
    }

    @Override
    public long getTotalPhysical() {
        return totalPhysical;
    }

    public void setTotalPhysical(long totalPhysical) {
        this.totalPhysical = totalPhysical;
    }

    @Override
    public long getFreePhysical() {
        return freePhysical;
    }

    public void setFreePhysical(long freePhysical) {
        this.freePhysical = freePhysical;
    }

    @Override
    public long getMaxNative() {
        return maxNativeMemory;
    }

    public void setMaxNativeMemory(long maxNativeMemory) {
        this.maxNativeMemory = maxNativeMemory;
    }

    @Override
    public long getCommittedNative() {
        return committedNativeMemory;
    }

    public void setCommittedNativeMemory(long committed) {
        this.committedNativeMemory = committed;
    }

    @Override
    public long getUsedNative() {
        return usedNativeMemory;
    }

    public void setUsedNativeMemory(long used) {
        this.usedNativeMemory = used;
    }

    @Override
    public long getFreeNative() {
        return freeNativeMemory;
    }

    public void setFreeNativeMemory(long freeNativeMemory) {
        this.freeNativeMemory = freeNativeMemory;
    }

    @Override
    public long getMaxMetadata() {
        return maxMetadata;
    }

    public void setMaxMetadata(long maxMetadata) {
        this.maxMetadata = maxMetadata;
    }

    @Override
    public long getUsedMetadata() {
        return usedMetadata;
    }

    public void setUsedMetadata(long usedMetadata) {
        this.usedMetadata = usedMetadata;
    }

    @Override
    public long getMaxHeap() {
        return maxHeap;
    }

    @Override
    public long getCommittedHeap() {
        return committedHeap;
    }

    @Override
    public long getUsedHeap() {
        return usedHeap;
    }

    public void setMaxHeap(long maxHeap) {
        this.maxHeap = maxHeap;
    }

    public void setCommittedHeap(long committedHeap) {
        this.committedHeap = committedHeap;
    }

    public void setUsedHeap(long usedHeap) {
        this.usedHeap = usedHeap;
    }

    @Override
    public long getFreeHeap() {
        return maxHeap - usedHeap;
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
        root.add(JSON_CREATION_TIME, creationTime);
        root.add(JSON_TOTAL_PHYSICAL, totalPhysical);
        root.add(JSON_FREE_PHYSICAL, freePhysical);
        root.add(JSON_MAX_NATIVE_MEMORY, maxNativeMemory);
        root.add(JSON_COMMITTED_NATIVE_MEMORY, committedNativeMemory);
        root.add(JSON_USED_NATIVE_MEMORY, usedNativeMemory);
        root.add(JSON_FREE_NATIVE_MEMORY, freeNativeMemory);
        root.add(JSON_MAX_HEAP, maxHeap);
        root.add(JSON_COMMITTED_HEAP, committedHeap);
        root.add(JSON_USED_HEAP, usedHeap);
        if (gcStats == null) {
            gcStats = new LocalGCStatsImpl();
        }
        root.add(JSON_GC_STATS, gcStats.toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, JSON_CREATION_TIME, -1L);
        totalPhysical = getLong(json, JSON_TOTAL_PHYSICAL, -1L);
        freePhysical = getLong(json, JSON_FREE_PHYSICAL, -1L);
        maxNativeMemory = getLong(json, JSON_MAX_NATIVE_MEMORY, -1L);
        committedNativeMemory = getLong(json, JSON_COMMITTED_NATIVE_MEMORY, -1L);
        usedNativeMemory = getLong(json, JSON_USED_NATIVE_MEMORY, -1L);
        freeNativeMemory = getLong(json, JSON_FREE_NATIVE_MEMORY, -1L);
        maxHeap = getLong(json, JSON_MAX_HEAP, -1L);
        committedHeap = getLong(json, JSON_COMMITTED_HEAP, -1L);
        usedHeap = getLong(json, JSON_USED_HEAP, -1L);
        gcStats = new LocalGCStatsImpl();
        if (json.get(JSON_GC_STATS) != null) {
            gcStats.fromJson(getObject(json, JSON_GC_STATS));
        }
    }

    @Override
    public String toString() {
        return "LocalMemoryStats{"
                + "totalPhysical=" + totalPhysical
                + ", freePhysical=" + freePhysical
                + ", maxNativeMemory=" + maxNativeMemory
                + ", committedNativeMemory=" + committedNativeMemory
                + ", usedNativeMemory=" + usedNativeMemory
                + ", maxMetadata=" + maxMetadata
                + ", usedUsedMetadata=" + usedMetadata
                + ", maxHeap=" + maxHeap
                + ", committedHeap=" + committedHeap
                + ", usedHeap=" + usedHeap
                + ", gcStats=" + gcStats
                + '}';
    }
}
