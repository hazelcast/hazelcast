/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.eclipsesource.json.JsonValue;
import com.hazelcast.internal.diagnostics.Distribution;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.util.Clock;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.ConcurrencyUtil.setMax;
import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * Default implementation of {@link LocalMapStats}
 */
public class LocalMapStatsImpl implements LocalMapStats {

    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> LAST_ACCESS_TIME =
            newUpdater(LocalMapStatsImpl.class, "lastAccessTime");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> LAST_UPDATE_TIME =
            newUpdater(LocalMapStatsImpl.class, "lastUpdateTime");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> NUMBER_OF_OTHER_OPERATIONS =
            newUpdater(LocalMapStatsImpl.class, "numberOfOtherOperations");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> NUMBER_OF_EVENTS =
            newUpdater(LocalMapStatsImpl.class, "numberOfEvents");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> GET_COUNT =
            newUpdater(LocalMapStatsImpl.class, "getCount");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> PUT_COUNT =
            newUpdater(LocalMapStatsImpl.class, "putCount");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> REMOVE_COUNT =
            newUpdater(LocalMapStatsImpl.class, "removeCount");

    private final Distribution getLatencyDistribution = new Distribution(32,1000);
    private final Distribution putLatencyDistribution = new Distribution(32,1000);
    private final Distribution removeLatencyDistribution = new Distribution(32,1000);
    private final Distribution queryLatencyDistribution = new Distribution(32,1000);

    // These fields are only accessed through the updaters
    @Probe
    private volatile long lastAccessTime;
    @Probe
    private volatile long lastUpdateTime;
    @Probe
    private volatile long hits;
    @Probe
    private volatile long numberOfOtherOperations;
    @Probe
    private volatile long numberOfEvents;
    @Probe
    private volatile long getCount;
    @Probe
    private volatile long putCount;
    @Probe
    private volatile long removeCount;

    @Probe
    private volatile long creationTime;
    @Probe
    private volatile long ownedEntryCount;
    @Probe
    private volatile long backupEntryCount;
    @Probe
    private volatile long ownedEntryMemoryCost;
    @Probe
    private volatile long backupEntryMemoryCost;
    /**
     * Holds total heap cost of map & Near Cache & backups.
     */
    @Probe
    private volatile long heapCost;
    @Probe
    private volatile long lockedEntryCount;
    @Probe
    private volatile long dirtyEntryCount;
    @Probe
    private volatile int backupCount;

    private volatile NearCacheStats nearCacheStats;

    public Distribution getGetLatencyDistribution() {
        return getLatencyDistribution;
    }

    public Distribution getPutLatencyDistribution() {
        return putLatencyDistribution;
    }

    public Distribution getRemoveLatencyDistribution() {
        return removeLatencyDistribution;
    }

    public Distribution getQueryLatencyDistribution() {
        return queryLatencyDistribution;
    }

    public LocalMapStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount = ownedEntryCount;
    }

    @Override
    public long getBackupEntryCount() {
        return backupEntryCount;
    }

    public void setBackupEntryCount(long backupEntryCount) {
        this.backupEntryCount = backupEntryCount;
    }

    @Override
    public int getBackupCount() {
        return backupCount;
    }

    public void setBackupCount(int backupCount) {
        this.backupCount = backupCount;
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost;
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        this.ownedEntryMemoryCost = ownedEntryMemoryCost;
    }

    @Override
    public long getBackupEntryMemoryCost() {
        return backupEntryMemoryCost;
    }

    public void setBackupEntryMemoryCost(long backupEntryMemoryCost) {
        this.backupEntryMemoryCost = backupEntryMemoryCost;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        setMax(this, LAST_ACCESS_TIME, lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        setMax(this, LAST_UPDATE_TIME, lastUpdateTime);
    }

    @Override
    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    @Override
    public long getLockedEntryCount() {
        return lockedEntryCount;
    }

    public void setLockedEntryCount(long lockedEntryCount) {
        this.lockedEntryCount = lockedEntryCount;
    }

    @Override
    public long getDirtyEntryCount() {
        return dirtyEntryCount;
    }

    public void setDirtyEntryCount(long dirtyEntryCount) {
        this.dirtyEntryCount = dirtyEntryCount;
    }

    @Override
    public long total() {
        return putCount + getCount + removeCount + numberOfOtherOperations;
    }

    @Override
    public long getPutOperationCount() {
        return putCount;
    }

    public void incrementPutLatencyNanos(long latencyNanos) {
        incrementPutLatencyNanos(1, latencyNanos);
    }

    public void incrementQueryLatencyNanos(long latencyNanos) {
        queryLatencyDistribution.record(latencyNanos);
    }

    public void incrementPutLatencyNanos(long delta, long latencyNanos) {
        PUT_COUNT.addAndGet(this, delta);
        putLatencyDistribution.record(new Random().nextInt((int)TimeUnit.SECONDS.toNanos(5)));
    }

    @Override
    public long getGetOperationCount() {
        return getCount;
    }

    public void incrementGetLatencyNanos(long latencyNanos) {
        incrementGetLatencyNanos(1, latencyNanos);
    }

    public void incrementGetLatencyNanos(long delta, long latencyNanos) {
        GET_COUNT.addAndGet(this, delta);
        getLatencyDistribution.record(latencyNanos);
    }

    @Override
    public long getRemoveOperationCount() {
        return removeCount;
    }

    public void incrementRemoveLatencyNanos(long latencyNanos) {
        REMOVE_COUNT.incrementAndGet(this);
        removeLatencyDistribution.record(latencyNanos);
    }

    @Probe
    @Override
    public long getTotalPutLatency() {
        return NANOSECONDS.toMillis(putLatencyDistribution.total());
    }

    @Probe
    @Override
    public long getTotalGetLatency() {
        return NANOSECONDS.toMillis(getLatencyDistribution.total());
    }

    @Probe
    @Override
    public long getTotalRemoveLatency() {
        return NANOSECONDS.toMillis(removeLatencyDistribution.total());
    }

    @Probe
    @Override
    public long getMaxPutLatency() {
        return NANOSECONDS.toMillis(putLatencyDistribution.max());
    }

    @Probe
    @Override
    public long getMaxGetLatency() {
        return NANOSECONDS.toMillis(getLatencyDistribution.max());
    }

    @Probe
    @Override
    public long getMaxRemoveLatency() {
        return NANOSECONDS.toMillis(removeLatencyDistribution.max());
    }

    @Override
    public long getOtherOperationCount() {
        return numberOfOtherOperations;
    }

    public void incrementOtherOperations() {
        NUMBER_OF_OTHER_OPERATIONS.incrementAndGet(this);
    }

    @Override
    public long getEventOperationCount() {
        return numberOfEvents;
    }

    public void incrementReceivedEvents() {
        NUMBER_OF_EVENTS.incrementAndGet(this);
    }

    @Override
    public long getHeapCost() {
        return heapCost;
    }

    public void setHeapCost(long heapCost) {
        this.heapCost = heapCost;
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheStats;
    }

    public void setNearCacheStats(NearCacheStats nearCacheStats) {
        this.nearCacheStats = nearCacheStats;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("getCount", getCount);
        root.add("putCount", putCount);
        root.add("removeCount", removeCount);
        root.add("numberOfOtherOperations", numberOfOtherOperations);
        root.add("numberOfEvents", numberOfEvents);
        root.add("lastAccessTime", lastAccessTime);
        root.add("lastUpdateTime", lastUpdateTime);
        root.add("hits", hits);
        root.add("ownedEntryCount", ownedEntryCount);
        root.add("backupEntryCount", backupEntryCount);
        root.add("backupCount", backupCount);
        root.add("ownedEntryMemoryCost", ownedEntryMemoryCost);
        root.add("backupEntryMemoryCost", backupEntryMemoryCost);
        root.add("creationTime", creationTime);
        root.add("lockedEntryCount", lockedEntryCount);
        root.add("dirtyEntryCount", dirtyEntryCount);

        // keep the contract as milliseconds for latencies sent using Json
        root.add("totalGetLatencies", NANOSECONDS.toMillis(getLatencyDistribution.total()));
        root.add("totalPutLatencies", NANOSECONDS.toMillis(putLatencyDistribution.total()));
        root.add("totalRemoveLatencies", NANOSECONDS.toMillis(removeLatencyDistribution.total()));
        root.add("maxGetLatency", NANOSECONDS.toMillis(getLatencyDistribution.max()));
        root.add("maxPutLatency", NANOSECONDS.toMillis(putLatencyDistribution.max()));
        root.add("maxRemoveLatency", NANOSECONDS.toMillis(removeLatencyDistribution.max()));

        root.add("heapCost", heapCost);
        if (nearCacheStats != null) {
            root.add("nearCacheStats", nearCacheStats.toJson());
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        getCount = getLong(json, "getCount", -1L);
        putCount = getLong(json, "putCount", -1L);
        removeCount = getLong(json, "removeCount", -1L);
        numberOfOtherOperations = getLong(json, "numberOfOtherOperations", -1L);
        numberOfEvents = getLong(json, "numberOfEvents", -1L);
        lastAccessTime = getLong(json, "lastAccessTime", -1L);
        lastUpdateTime = getLong(json, "lastUpdateTime", -1L);

        // Json uses milliseconds but we keep latencies in nanoseconds internally
//        totalGetLatenciesNanos = MILLISECONDS.toNanos(getLong(json, "totalGetLatencies", -1L));
//        totalPutLatenciesNanos = MILLISECONDS.toNanos(getLong(json, "totalPutLatencies", -1L));
//        totalRemoveLatenciesNanos = MILLISECONDS.toNanos(getLong(json, "totalRemoveLatencies", -1L));
//        maxGetLatency = MILLISECONDS.toNanos(getLong(json, "maxGetLatency", -1L));
//        maxPutLatency = MILLISECONDS.toNanos(getLong(json, "maxPutLatency", -1L));
//        maxRemoveLatency = MILLISECONDS.toNanos(getLong(json, "maxRemoveLatency", -1L));

        hits = getLong(json, "hits", -1L);
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        backupEntryCount = getLong(json, "backupEntryCount", -1L);
        backupCount = getInt(json, "backupCount", -1);
        ownedEntryMemoryCost = getLong(json, "ownedEntryMemoryCost", -1L);
        backupEntryMemoryCost = getLong(json, "backupEntryMemoryCost", -1L);
        creationTime = getLong(json, "creationTime", -1L);
        lockedEntryCount = getLong(json, "lockedEntryCount", -1L);
        dirtyEntryCount = getLong(json, "dirtyEntryCount", -1L);
        heapCost = getLong(json, "heapCost", -1L);
        JsonValue jsonNearCacheStats = json.get("nearCacheStats");
        if (jsonNearCacheStats != null) {
            nearCacheStats = new NearCacheStatsImpl();
            nearCacheStats.fromJson(jsonNearCacheStats.asObject());
        }
    }

    @Override
    public String toString() {
        return "LocalMapStatsImpl{"
                + "lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", hits=" + hits
                + ", numberOfOtherOperations=" + numberOfOtherOperations
                + ", numberOfEvents=" + numberOfEvents
                + ", getCount=" + getCount
                + ", putCount=" + putCount
                + ", removeCount=" + removeCount
//                + ", totalGetLatencies=" + NANOSECONDS.toMillis(totalGetLatenciesNanos)
//                + ", totalPutLatencies=" + NANOSECONDS.toMillis(totalPutLatenciesNanos)
//                + ", totalRemoveLatencies=" + NANOSECONDS.toMillis(totalRemoveLatenciesNanos)
//                + ", maxGetLatency=" + NANOSECONDS.toMillis(maxGetLatency)
//                + ", maxPutLatency=" + NANOSECONDS.toMillis(maxPutLatency)
//                + ", maxRemoveLatency=" + NANOSECONDS.toMillis(maxRemoveLatency)
                + ", ownedEntryCount=" + ownedEntryCount
                + ", backupEntryCount=" + backupEntryCount
                + ", backupCount=" + backupCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", backupEntryMemoryCost=" + backupEntryMemoryCost
                + ", creationTime=" + creationTime
                + ", lockedEntryCount=" + lockedEntryCount
                + ", dirtyEntryCount=" + dirtyEntryCount
                + ", heapCost=" + heapCost
                + ", nearCacheStats=" + (nearCacheStats != null ? nearCacheStats : "")
                + '}';
    }
}
