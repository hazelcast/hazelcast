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
import com.hazelcast.internal.json.JsonObject.Member;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.query.LocalIndexStats;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getLong;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.TimeUtil.timeInMsOrOneIfResultIsZero;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * Default implementation of {@link LocalMapStats}
 */
@SuppressWarnings({"checkstyle:methodcount"})
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
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> SET_COUNT =
            newUpdater(LocalMapStatsImpl.class, "setCount");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> REMOVE_COUNT =
            newUpdater(LocalMapStatsImpl.class, "removeCount");

    // The resolution is in nano seconds for the following latencies
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_GET_LATENCIES =
            newUpdater(LocalMapStatsImpl.class, "totalGetLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_PUT_LATENCIES =
            newUpdater(LocalMapStatsImpl.class, "totalPutLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_SET_LATENCIES =
            newUpdater(LocalMapStatsImpl.class, "totalSetLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_REMOVE_LATENCIES =
            newUpdater(LocalMapStatsImpl.class, "totalRemoveLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_GET_LATENCY =
            newUpdater(LocalMapStatsImpl.class, "maxGetLatency");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_PUT_LATENCY =
            newUpdater(LocalMapStatsImpl.class, "maxPutLatency");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_SET_LATENCY =
            newUpdater(LocalMapStatsImpl.class, "maxSetLatency");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_REMOVE_LATENCY =
            newUpdater(LocalMapStatsImpl.class, "maxRemoveLatency");

    private final ConcurrentMap<String, LocalIndexStatsImpl> mutableIndexStats =
            new ConcurrentHashMap<String, LocalIndexStatsImpl>();
    private final Map<String, LocalIndexStats> indexStats = Collections.<String, LocalIndexStats>unmodifiableMap(
            mutableIndexStats);

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
    private volatile long setCount;
    @Probe
    private volatile long removeCount;
    private volatile long totalGetLatenciesNanos;
    private volatile long totalPutLatenciesNanos;
    private volatile long totalSetLatenciesNanos;
    private volatile long totalRemoveLatenciesNanos;
    private volatile long maxGetLatency;
    private volatile long maxPutLatency;
    private volatile long maxSetLatency;
    private volatile long maxRemoveLatency;
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
     * Holds total heap cost of map & Near Cache & backups & Merkle trees.
     */
    @Probe
    private volatile long heapCost;
    /**
     * Holds the total memory footprint of the Merkle trees
     */
    @Probe
    private volatile long merkleTreesCost;
    @Probe
    private volatile long lockedEntryCount;
    @Probe
    private volatile long dirtyEntryCount;
    @Probe
    private volatile int backupCount;
    private volatile NearCacheStats nearCacheStats;
    @Probe
    private volatile long queryCount;
    @Probe
    private volatile long indexedQueryCount;

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
    public long getPutOperationCount() {
        return putCount;
    }

    @Override
    public long getSetOperationCount() {
        return setCount;
    }

    @Override
    public long getGetOperationCount() {
        return getCount;
    }

    @Override
    public long getRemoveOperationCount() {
        return removeCount;
    }

    @Probe
    @Override
    public long getTotalPutLatency() {
        return convertNanosToMillis(totalPutLatenciesNanos);
    }

    @Probe
    @Override
    public long getTotalSetLatency() {
        return convertNanosToMillis(totalSetLatenciesNanos);
    }

    @Probe
    @Override
    public long getTotalGetLatency() {
        return convertNanosToMillis(totalGetLatenciesNanos);
    }

    @Probe
    @Override
    public long getTotalRemoveLatency() {
        return convertNanosToMillis(totalRemoveLatenciesNanos);
    }

    @Probe
    @Override
    public long getMaxPutLatency() {
        return convertNanosToMillis(maxPutLatency);
    }

    @Probe
    @Override
    public long getMaxSetLatency() {
        return convertNanosToMillis(maxSetLatency);
    }

    @Probe
    @Override
    public long getMaxGetLatency() {
        return convertNanosToMillis(maxGetLatency);
    }

    @Probe
    @Override
    public long getMaxRemoveLatency() {
        return convertNanosToMillis(maxRemoveLatency);
    }

    @Override
    public long getEventOperationCount() {
        return numberOfEvents;
    }

    @Override
    public long getOtherOperationCount() {
        return numberOfOtherOperations;
    }

    @Override
    public long total() {
        return putCount + getCount + removeCount + numberOfOtherOperations;
    }

    @Override
    public long getHeapCost() {
        return heapCost;
    }

    public void setHeapCost(long heapCost) {
        this.heapCost = heapCost;
    }

    @Override
    public long getMerkleTreesCost() {
        return merkleTreesCost;
    }

    public void setMerkleTreesCost(long merkleTreeCost) {
        this.merkleTreesCost = merkleTreeCost;
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheStats;
    }

    public void setNearCacheStats(NearCacheStats nearCacheStats) {
        this.nearCacheStats = nearCacheStats;
    }

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    /**
     * Sets the query count of this stats to the given query count value.
     *
     * @param queryCount the query count value to set.
     */
    public void setQueryCount(long queryCount) {
        this.queryCount = queryCount;
    }

    @Override
    public long getIndexedQueryCount() {
        return indexedQueryCount;
    }

    /**
     * Sets the indexed query count of this stats to the given indexed query
     * count value.
     *
     * @param indexedQueryCount the indexed query count value to set.
     */
    public void setIndexedQueryCount(long indexedQueryCount) {
        this.indexedQueryCount = indexedQueryCount;
    }

    @Override
    public Map<String, LocalIndexStats> getIndexStats() {
        return indexStats;
    }

    /**
     * Sets the per-index stats of this map stats to the given per-index stats.
     *
     * @param indexStats the per-index stats to set.
     */
    public void setIndexStats(Map<String, LocalIndexStatsImpl> indexStats) {
        this.mutableIndexStats.clear();
        if (indexStats != null) {
            this.mutableIndexStats.putAll(indexStats);
        }
    }

    public void incrementPutLatencyNanos(long latencyNanos) {
        incrementPutLatencyNanos(1, latencyNanos);
    }

    public void incrementPutLatencyNanos(long delta, long latencyNanos) {
        PUT_COUNT.addAndGet(this, delta);
        TOTAL_PUT_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_PUT_LATENCY, latencyNanos);
    }

    public void incrementSetLatencyNanos(long latencyNanos) {
        SET_COUNT.incrementAndGet(this);
        TOTAL_SET_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_SET_LATENCY, latencyNanos);
    }

    public void incrementGetLatencyNanos(long latencyNanos) {
        incrementGetLatencyNanos(1, latencyNanos);
    }

    public void incrementGetLatencyNanos(long delta, long latencyNanos) {
        GET_COUNT.addAndGet(this, delta);
        TOTAL_GET_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_GET_LATENCY, latencyNanos);
    }

    public void incrementRemoveLatencyNanos(long latencyNanos) {
        REMOVE_COUNT.incrementAndGet(this);
        TOTAL_REMOVE_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_REMOVE_LATENCY, latencyNanos);
    }

    public void incrementOtherOperations() {
        NUMBER_OF_OTHER_OPERATIONS.incrementAndGet(this);
    }

    public void incrementReceivedEvents() {
        NUMBER_OF_EVENTS.incrementAndGet(this);
    }

    public void updateIndexStats(Map<String, OnDemandIndexStats> freshIndexStats) {
        // A new index can be added, but already existing indexes can't be
        // removed, that matches the current implementation properties of the
        // index management.

        if (freshIndexStats == null) {
            // no indexes yet
            return;
        }

        for (Map.Entry<String, OnDemandIndexStats> freshIndexEntry : freshIndexStats.entrySet()) {
            String indexName = freshIndexEntry.getKey();
            LocalIndexStatsImpl indexStats = mutableIndexStats.get(indexName);
            if (indexStats == null) {
                indexStats = new LocalIndexStatsImpl();
                indexStats.setAllFrom(freshIndexEntry.getValue());
                mutableIndexStats.putIfAbsent(indexName, indexStats);
            } else {
                indexStats.setAllFrom(freshIndexEntry.getValue());
            }
        }
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("getCount", getCount);
        root.add("putCount", putCount);
        root.add("setCount", setCount);
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
        root.add("totalGetLatencies", convertNanosToMillis(totalGetLatenciesNanos));
        root.add("totalPutLatencies", convertNanosToMillis(totalPutLatenciesNanos));
        root.add("totalSetLatencies", convertNanosToMillis(totalSetLatenciesNanos));
        root.add("totalRemoveLatencies", convertNanosToMillis(totalRemoveLatenciesNanos));
        root.add("maxGetLatency", convertNanosToMillis(maxGetLatency));
        root.add("maxPutLatency", convertNanosToMillis(maxPutLatency));
        root.add("maxSetLatency", convertNanosToMillis(maxSetLatency));
        root.add("maxRemoveLatency", convertNanosToMillis(maxRemoveLatency));

        root.add("heapCost", heapCost);
        root.add("merkleTreesCost", merkleTreesCost);
        if (nearCacheStats != null) {
            root.add("nearCacheStats", nearCacheStats.toJson());
        }

        root.add("queryCount", queryCount);
        root.add("indexedQueryCount", indexedQueryCount);
        Map<String, LocalIndexStats> localIndexStats = indexStats;
        if (!localIndexStats.isEmpty()) {
            JsonObject indexes = new JsonObject();
            for (Map.Entry<String, LocalIndexStats> indexEntry : localIndexStats.entrySet()) {
                indexes.add(indexEntry.getKey(), indexEntry.getValue().toJson());
            }
            root.add("indexStats", indexes);
        }

        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        getCount = getLong(json, "getCount", -1L);
        putCount = getLong(json, "putCount", -1L);
        setCount = getLong(json, "setCount", -1L);
        removeCount = getLong(json, "removeCount", -1L);
        numberOfOtherOperations = getLong(json, "numberOfOtherOperations", -1L);
        numberOfEvents = getLong(json, "numberOfEvents", -1L);
        lastAccessTime = getLong(json, "lastAccessTime", -1L);
        lastUpdateTime = getLong(json, "lastUpdateTime", -1L);

        // Json uses milliseconds but we keep latencies in nanoseconds internally
        totalGetLatenciesNanos = convertMillisToNanos(getLong(json, "totalGetLatencies", -1L));
        totalPutLatenciesNanos = convertMillisToNanos(getLong(json, "totalPutLatencies", -1L));
        totalSetLatenciesNanos = convertMillisToNanos(getLong(json, "totalSetLatencies", -1L));
        totalRemoveLatenciesNanos = convertMillisToNanos(getLong(json, "totalRemoveLatencies", -1L));
        maxGetLatency = convertMillisToNanos(getLong(json, "maxGetLatency", -1L));
        maxPutLatency = convertMillisToNanos(getLong(json, "maxPutLatency", -1L));
        maxSetLatency = convertMillisToNanos(getLong(json, "maxSetLatency", -1L));
        maxRemoveLatency = convertMillisToNanos(getLong(json, "maxRemoveLatency", -1L));

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
        merkleTreesCost = getLong(json, "merkleTreesCost", -1L);
        JsonValue jsonNearCacheStats = json.get("nearCacheStats");
        if (jsonNearCacheStats != null) {
            nearCacheStats = new NearCacheStatsImpl();
            nearCacheStats.fromJson(jsonNearCacheStats.asObject());
        }

        queryCount = getLong(json, "queryCount", -1L);
        indexedQueryCount = getLong(json, "indexedQueryCount", -1L);
        JsonObject indexes = getObject(json, "indexStats", null);
        if (indexes != null && !indexes.isEmpty()) {
            Map<String, LocalIndexStatsImpl> localIndexStats = new HashMap<String, LocalIndexStatsImpl>();
            for (Member member : indexes) {
                LocalIndexStatsImpl indexStats = new LocalIndexStatsImpl();
                indexStats.fromJson(member.getValue().asObject());
                localIndexStats.put(member.getName(), indexStats);
            }
            setIndexStats(localIndexStats);
        } else {
            setIndexStats(null);
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
                + ", setCount=" + setCount
                + ", removeCount=" + removeCount
                + ", totalGetLatencies=" + convertNanosToMillis(totalGetLatenciesNanos)
                + ", totalPutLatencies=" + convertNanosToMillis(totalPutLatenciesNanos)
                + ", totalSetLatencies=" + convertNanosToMillis(totalSetLatenciesNanos)
                + ", totalRemoveLatencies=" + convertNanosToMillis(totalRemoveLatenciesNanos)
                + ", maxGetLatency=" + convertNanosToMillis(maxGetLatency)
                + ", maxPutLatency=" + convertNanosToMillis(maxPutLatency)
                + ", maxSetLatency=" + convertNanosToMillis(maxSetLatency)
                + ", maxRemoveLatency=" + convertNanosToMillis(maxRemoveLatency)
                + ", ownedEntryCount=" + ownedEntryCount
                + ", backupEntryCount=" + backupEntryCount
                + ", backupCount=" + backupCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", backupEntryMemoryCost=" + backupEntryMemoryCost
                + ", creationTime=" + creationTime
                + ", lockedEntryCount=" + lockedEntryCount
                + ", dirtyEntryCount=" + dirtyEntryCount
                + ", heapCost=" + heapCost
                + ", merkleTreesCost=" + merkleTreesCost
                + ", nearCacheStats=" + (nearCacheStats != null ? nearCacheStats : "")
                + ", queryCount=" + queryCount
                + ", indexedQueryCount=" + indexedQueryCount
                + ", indexStats=" + indexStats
                + '}';
    }

    private static long convertNanosToMillis(long nanos) {
        return timeInMsOrOneIfResultIsZero(nanos, NANOSECONDS);
    }

    private static long convertMillisToNanos(long millis) {
        return MILLISECONDS.toNanos(millis);
    }
}
