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
import com.hazelcast.internal.memory.GarbageCollectorStats;
import com.hazelcast.internal.monitor.LocalGCStats;
import com.hazelcast.internal.util.Clock;

import static com.hazelcast.internal.util.JsonUtil.getLong;

public class LocalGCStatsImpl implements LocalGCStats {

    private long creationTime;
    private long minorCount;
    private long minorTime;
    private long majorCount;
    private long majorTime;
    private long unknownCount;
    private long unknownTime;

    public LocalGCStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    public LocalGCStatsImpl(GarbageCollectorStats gcStats) {
        setMajorCount(gcStats.getMajorCollectionCount());
        setMajorTime(gcStats.getMajorCollectionTime());
        setMinorCount(gcStats.getMinorCollectionCount());
        setMinorTime(gcStats.getMinorCollectionTime());
        setUnknownCount(gcStats.getUnknownCollectionCount());
        setUnknownTime(gcStats.getUnknownCollectionTime());
    }

    @Override
    public long getMajorCollectionCount() {
        return majorCount;
    }

    @Override
    public long getMajorCollectionTime() {
        return majorTime;
    }

    @Override
    public long getMinorCollectionCount() {
        return minorCount;
    }

    @Override
    public long getMinorCollectionTime() {
        return minorTime;
    }

    @Override
    public long getUnknownCollectionCount() {
        return unknownCount;
    }

    @Override
    public long getUnknownCollectionTime() {
        return unknownTime;
    }

    public void setMinorCount(long minorCount) {
        this.minorCount = minorCount;
    }

    public void setMinorTime(long minorTime) {
        this.minorTime = minorTime;
    }

    public void setMajorCount(long majorCount) {
        this.majorCount = majorCount;
    }

    public void setMajorTime(long majorTime) {
        this.majorTime = majorTime;
    }

    public void setUnknownCount(long unknownCount) {
        this.unknownCount = unknownCount;
    }

    public void setUnknownTime(long unknownTime) {
        this.unknownTime = unknownTime;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("minorCount", minorCount);
        root.add("minorTime", minorTime);
        root.add("majorCount", majorCount);
        root.add("majorTime", majorTime);
        root.add("unknownCount", unknownCount);
        root.add("unknownTime", unknownTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        minorCount = getLong(json, "minorCount", -1L);
        minorTime = getLong(json, "minorTime", -1L);
        majorCount = getLong(json, "majorCount", -1L);
        majorTime = getLong(json, "majorTime", -1L);
        unknownCount = getLong(json, "unknownCount", -1L);
        unknownTime = getLong(json, "unknownTime", -1L);
    }

    @Override
    public String toString() {
        return "LocalGCStats{"
                + "creationTime=" + creationTime
                + ", minorCount=" + minorCount
                + ", minorTime=" + minorTime
                + ", majorCount=" + majorCount
                + ", majorTime=" + majorTime
                + ", unknownCount=" + unknownCount
                + ", unknownTime=" + unknownTime
                + '}';
    }
}
