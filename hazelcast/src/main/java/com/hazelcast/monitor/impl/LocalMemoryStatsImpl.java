/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.monitor.LocalGCStats;
import com.hazelcast.monitor.LocalMemoryStats;

import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getObject;

public class LocalMemoryStatsImpl implements LocalMemoryStats, JsonSerializable {

    private long creationTime;

    private long totalPhysical;

    private long freePhysical;

    private long maxNativeMemory;

    private long committedNativeMemory;

    private long usedNativeMemory;

    private long freeNativeMemory;

    private long maxHeap;

    private long committedHeap;

    private long usedHeap;

    private LocalGCStatsImpl gcStats;

    public LocalMemoryStatsImpl() {
    }

    public LocalMemoryStatsImpl(MemoryStats memoryStats) {
        setTotalPhysical(memoryStats.getTotalPhysical());
        setFreePhysical(memoryStats.getFreePhysical());
        setMaxNativeMemory(memoryStats.getMaxNativeMemory());
        setCommittedNativeMemory(memoryStats.getCommittedNativeMemory());
        setUsedNativeMemory(memoryStats.getUsedNativeMemory());
        setFreeNativeMemory(memoryStats.getFreeNativeMemory());
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
    public long getMaxNativeMemory() {
        return maxNativeMemory;
    }

    public void setMaxNativeMemory(long maxNativeMemory) {
        this.maxNativeMemory = maxNativeMemory;
    }

    @Override
    public long getCommittedNativeMemory() {
        return committedNativeMemory;
    }

    public void setCommittedNativeMemory(long allocated) {
        this.committedNativeMemory = allocated;
    }

    @Override
    public long getUsedNativeMemory() {
        return usedNativeMemory;
    }

    public void setUsedNativeMemory(long used) {
        this.usedNativeMemory = used;
    }

    @Override
    public long getFreeNativeMemory() {
        return freeNativeMemory;
    }

    public void setFreeNativeMemory(long freeNativeMemory) {
        this.freeNativeMemory = freeNativeMemory;
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

    public void setGcStats(LocalGCStatsImpl gcStats) {
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
        root.add("maxNativeMemory", maxNativeMemory);
        root.add("committedNativeMemory", committedNativeMemory);
        root.add("usedNativeMemory", usedNativeMemory);
        root.add("freeNativeMemory", freeNativeMemory);
        root.add("maxHeap", maxHeap);
        root.add("committedHeap", committedHeap);
        root.add("usedHeap", usedHeap);
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
        maxNativeMemory = getLong(json, "maxNativeMemory", -1L);
        committedNativeMemory = getLong(json, "committedNativeMemory", -1L);
        usedNativeMemory = getLong(json, "usedNativeMemory", -1L);
        freeNativeMemory = getLong(json, "freeNativeMemory", -1L);
        maxHeap = getLong(json, "maxHeap", -1L);
        committedHeap = getLong(json, "committedHeap", -1L);
        usedHeap = getLong(json, "usedHeap", -1L);
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
                + ", maxNativeMemory=" + maxNativeMemory
                + ", committedNativeMemory=" + committedNativeMemory
                + ", usedNativeMemory=" + usedNativeMemory
                + ", maxHeap=" + maxHeap
                + ", committedHeap=" + committedHeap
                + ", usedHeap=" + usedHeap
                + ", gcStats=" + gcStats
                + '}';
    }
}
