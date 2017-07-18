/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.ConcurrencyUtil.setMax;
import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

    // The resolution is in nano seconds for the following latencies
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_GET_LATENCIES =
            newUpdater(LocalMapStatsImpl.class, "totalGetLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_PUT_LATENCIES =
            newUpdater(LocalMapStatsImpl.class, "totalPutLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_REMOVE_LATENCIES =
            newUpdater(LocalMapStatsImpl.class, "totalRemoveLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_GET_LATENCY =
            newUpdater(LocalMapStatsImpl.class, "maxGetLatency");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_PUT_LATENCY =
            newUpdater(LocalMapStatsImpl.class, "maxPutLatency");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_REMOVE_LATENCY =
            newUpdater(LocalMapStatsImpl.class, "maxRemoveLatency");

    // These fields are only accessed through the updaters
    private volatile long lastAccessTime;
    private volatile long lastUpdateTime;
    private volatile long hits;
    private volatile long numberOfOtherOperations;
    private volatile long numberOfEvents;
    private volatile long getCount;
    private volatile long putCount;
    private volatile long removeCount;
    private volatile long totalGetLatenciesNanos;
    private volatile long totalPutLatenciesNanos;
    private volatile long totalRemoveLatenciesNanos;
    private volatile long maxGetLatency;
    private volatile long maxPutLatency;
    private volatile long maxRemoveLatency;

    private volatile long creationTime;
    private volatile long ownedEntryCount;
    private volatile long backupEntryCount;
    private volatile long ownedEntryMemoryCost;
    private volatile long backupEntryMemoryCost;
    /**
     * Holds total heap cost of map & Near Cache & backups.
     */
    private volatile long heapCost;
    private volatile long lockedEntryCount;
    private volatile long dirtyEntryCount;
    private volatile int backupCount;

    private volatile NearCacheStats nearCacheStats;

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

    public void incrementPutLatencyNanos(long delta, long latencyNanos) {
        PUT_COUNT.addAndGet(this, delta);
        TOTAL_PUT_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_PUT_LATENCY, latencyNanos);
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
        TOTAL_GET_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_GET_LATENCY, latencyNanos);
    }

    @Override
    public long getRemoveOperationCount() {
        return removeCount;
    }

    public void incrementRemoveLatencyNanos(long latencyNanos) {
        REMOVE_COUNT.incrementAndGet(this);
        TOTAL_REMOVE_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_REMOVE_LATENCY, latencyNanos);
    }

    @Override
    public long getTotalPutLatency() {
        return NANOSECONDS.toMillis(totalPutLatenciesNanos);
    }

    @Override
    public long getTotalGetLatency() {
        return NANOSECONDS.toMillis(totalGetLatenciesNanos);
    }

    @Override
    public long getTotalRemoveLatency() {
        return NANOSECONDS.toMillis(totalRemoveLatenciesNanos);
    }

    @Override
    public long getMaxPutLatency() {
        return NANOSECONDS.toMillis(maxPutLatency);
    }

    @Override
    public long getMaxGetLatency() {
        return NANOSECONDS.toMillis(maxGetLatency);
    }

    @Override
    public long getMaxRemoveLatency() {
        return NANOSECONDS.toMillis(maxRemoveLatency);
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
        root.add("totalGetLatencies", NANOSECONDS.toMillis(totalGetLatenciesNanos));
        root.add("totalPutLatencies", NANOSECONDS.toMillis(totalPutLatenciesNanos));
        root.add("totalRemoveLatencies", NANOSECONDS.toMillis(totalRemoveLatenciesNanos));
        root.add("maxGetLatency", NANOSECONDS.toMillis(maxGetLatency));
        root.add("maxPutLatency", NANOSECONDS.toMillis(maxPutLatency));
        root.add("maxRemoveLatency", NANOSECONDS.toMillis(maxRemoveLatency));

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
        totalGetLatenciesNanos = MILLISECONDS.toNanos(getLong(json, "totalGetLatencies", -1L));
        totalPutLatenciesNanos = MILLISECONDS.toNanos(getLong(json, "totalPutLatencies", -1L));
        totalRemoveLatenciesNanos = MILLISECONDS.toNanos(getLong(json, "totalRemoveLatencies", -1L));
        maxGetLatency = MILLISECONDS.toNanos(getLong(json, "maxGetLatency", -1L));
        maxPutLatency = MILLISECONDS.toNanos(getLong(json, "maxPutLatency", -1L));
        maxRemoveLatency = MILLISECONDS.toNanos(getLong(json, "maxRemoveLatency", -1L));

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
                + ", totalGetLatencies=" + NANOSECONDS.toMillis(totalGetLatenciesNanos)
                + ", totalPutLatencies=" + NANOSECONDS.toMillis(totalPutLatenciesNanos)
                + ", totalRemoveLatencies=" + NANOSECONDS.toMillis(totalRemoveLatenciesNanos)
                + ", maxGetLatency=" + NANOSECONDS.toMillis(maxGetLatency)
                + ", maxPutLatency=" + NANOSECONDS.toMillis(maxPutLatency)
                + ", maxRemoveLatency=" + NANOSECONDS.toMillis(maxRemoveLatency)
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
