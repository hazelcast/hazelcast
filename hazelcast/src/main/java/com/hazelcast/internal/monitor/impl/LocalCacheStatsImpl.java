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

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.monitor.LocalCacheStats;

import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.metrics.ProbeUnit.PERCENT;
import static com.hazelcast.internal.util.JsonUtil.getFloat;
import static com.hazelcast.internal.util.JsonUtil.getLong;

/**
 * Default implementation of {@link com.hazelcast.internal.monitor.LocalCacheStats}
 * <p>
 * This class just provides serialization/deserialization methods to be used in
 * {@link com.hazelcast.internal.monitor.MemberState} implementation while sending/receiving statistics to/from
 * Management center.
 * <p>
 * There are no calculations are done in this class, all statistics gathered from
 * {@link com.hazelcast.cache.CacheStatistics}
 * <p>
 * No setter methods are provided, all class fields supposed to be populated either
 * by a {@link com.hazelcast.cache.CacheStatistics} or while deserialization process
 * ({@link #fromJson(com.hazelcast.internal.json.JsonObject)}.
 *
 * @see com.hazelcast.cache.CacheStatistics
 */
public class LocalCacheStatsImpl implements LocalCacheStats {

    @Probe(unit = MS)
    private long creationTime;
    @Probe(unit = MS)
    private long lastAccessTime;
    @Probe(unit = MS)
    private long lastUpdateTime;
    @Probe
    private long ownedEntryCount;
    @Probe
    private long cacheHits;
    @Probe(unit = PERCENT)
    private float cacheHitPercentage;
    @Probe
    private long cacheMisses;
    @Probe(unit = PERCENT)
    private float cacheMissPercentage;
    @Probe
    private long cacheGets;
    @Probe
    private long cachePuts;
    @Probe
    private long cacheRemovals;
    @Probe
    private long cacheEvictions;
    @Probe(unit = MS)
    private float averageGetTime;
    @Probe(unit = MS)
    private float averagePutTime;
    @Probe(unit = MS)
    private float averageRemoveTime;

    public LocalCacheStatsImpl() {
    }

    public LocalCacheStatsImpl(CacheStatistics cacheStatistics) {
        creationTime = cacheStatistics.getCreationTime();
        lastAccessTime = cacheStatistics.getLastAccessTime();
        lastUpdateTime = cacheStatistics.getLastUpdateTime();
        ownedEntryCount = cacheStatistics.getOwnedEntryCount();
        cacheHits = cacheStatistics.getCacheHits();
        cacheHitPercentage = cacheStatistics.getCacheHitPercentage();
        cacheMisses = cacheStatistics.getCacheMisses();
        cacheMissPercentage = cacheStatistics.getCacheMissPercentage();
        cacheGets = cacheStatistics.getCacheGets();
        cachePuts = cacheStatistics.getCachePuts();
        cacheRemovals = cacheStatistics.getCacheRemovals();
        cacheEvictions = cacheStatistics.getCacheEvictions();
        averageGetTime = cacheStatistics.getAverageGetTime();
        averagePutTime = cacheStatistics.getAveragePutTime();
        averageRemoveTime = cacheStatistics.getAverageRemoveTime();
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    @Override
    public long getCacheHits() {
        return cacheHits;
    }

    @Override
    public float getCacheHitPercentage() {
        return cacheHitPercentage;
    }

    @Override
    public long getCacheMisses() {
        return cacheMisses;
    }

    @Override
    public float getCacheMissPercentage() {
        return cacheMissPercentage;
    }

    @Override
    public long getCacheGets() {
        return cacheGets;
    }

    @Override
    public long getCachePuts() {
        return cachePuts;
    }

    @Override
    public long getCacheRemovals() {
        return cacheRemovals;
    }

    @Override
    public long getCacheEvictions() {
        return cacheEvictions;
    }

    @Override
    public float getAverageGetTime() {
        return averageGetTime;
    }

    @Override
    public float getAveragePutTime() {
        return averagePutTime;
    }

    @Override
    public float getAverageRemoveTime() {
        return averageRemoveTime;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("lastAccessTime", lastAccessTime);
        root.add("lastUpdateTime", lastUpdateTime);
        root.add("ownedEntryCount", ownedEntryCount);
        root.add("cacheHits", cacheHits);
        root.add("cacheHitPercentage", cacheHitPercentage);
        root.add("cacheMisses", cacheMisses);
        root.add("cacheMissPercentage", cacheMissPercentage);
        root.add("cacheGets", cacheGets);
        root.add("cachePuts", cachePuts);
        root.add("cacheRemovals", cacheRemovals);
        root.add("cacheEvictions", cacheEvictions);
        root.add("averageGetTime", averageGetTime);
        root.add("averagePutTime", averagePutTime);
        root.add("averageRemoveTime", averageRemoveTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        lastAccessTime = getLong(json, "lastAccessTime", -1L);
        lastUpdateTime = getLong(json, "lastUpdateTime", -1L);
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        cacheHits = getLong(json, "cacheHits", -1L);
        cacheHitPercentage = getFloat(json, "cacheHitPercentage", -1f);
        cacheMisses = getLong(json, "cacheMisses", -1L);
        cacheMissPercentage = getFloat(json, "cacheMissPercentage", -1f);
        cacheGets = getLong(json, "cacheGets", -1L);
        cachePuts = getLong(json, "cachePuts", -1L);
        cacheRemovals = getLong(json, "cacheRemovals", -1L);
        cacheEvictions = getLong(json, "cacheEvictions", -1L);
        averageGetTime = getFloat(json, "averageGetTime", -1f);
        averagePutTime = getFloat(json, "averagePutTime", -1f);
        averageRemoveTime = getFloat(json, "averageRemoveTime", -1f);
    }

    @Override
    public String toString() {
        return "LocalCacheStatsImpl{"
                + "creationTime=" + creationTime
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", ownedEntryCount=" + ownedEntryCount
                + ", cacheHits=" + cacheHits
                + ", cacheHitPercentage=" + cacheHitPercentage
                + ", cacheMisses=" + cacheMisses
                + ", cacheMissPercentage=" + cacheMissPercentage
                + ", cacheGets=" + cacheGets
                + ", cachePuts=" + cachePuts
                + ", cacheRemovals=" + cacheRemovals
                + ", cacheEvictions=" + cacheEvictions
                + ", averageGetTime=" + averageGetTime
                + ", averagePutTime=" + averagePutTime
                + ", averageRemoveTime=" + averageRemoveTime
                + '}';
    }
}
