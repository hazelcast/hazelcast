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
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.internal.util.Clock;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;
import static com.hazelcast.internal.util.JsonUtil.getLong;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * This class collects statistics about the replication map usage for management center and is
 * able to transform those between wire format and instance view.
 */
@SuppressWarnings("checkstyle:methodcount")
public class LocalReplicatedMapStatsImpl implements LocalReplicatedMapStats {

    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> LAST_ACCESS_TIME =
            newUpdater(LocalReplicatedMapStatsImpl.class, "lastAccessTime");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> LAST_UPDATE_TIME =
            newUpdater(LocalReplicatedMapStatsImpl.class, "lastUpdateTime");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> HITS =
            newUpdater(LocalReplicatedMapStatsImpl.class, "hits");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> NUMBER_OF_OTHER_OPERATIONS =
            newUpdater(LocalReplicatedMapStatsImpl.class, "numberOfOtherOperations");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> NUMBER_OF_EVENTS =
            newUpdater(LocalReplicatedMapStatsImpl.class, "numberOfEvents");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> GET_COUNT =
            newUpdater(LocalReplicatedMapStatsImpl.class, "getCount");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> PUT_COUNT =
            newUpdater(LocalReplicatedMapStatsImpl.class, "putCount");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> REMOVE_COUNT =
            newUpdater(LocalReplicatedMapStatsImpl.class, "removeCount");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> TOTAL_GET_LATENCIES =
            newUpdater(LocalReplicatedMapStatsImpl.class, "totalGetLatencies");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> TOTAL_PUT_LATENCIES =
            newUpdater(LocalReplicatedMapStatsImpl.class, "totalPutLatencies");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> TOTAL_REMOVE_LATENCIES =
            newUpdater(LocalReplicatedMapStatsImpl.class, "totalRemoveLatencies");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_GET_LATENCY =
            newUpdater(LocalReplicatedMapStatsImpl.class, "maxGetLatency");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_PUT_LATENCY =
            newUpdater(LocalReplicatedMapStatsImpl.class, "maxPutLatency");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_REMOVE_LATENCY =
            newUpdater(LocalReplicatedMapStatsImpl.class, "maxRemoveLatency");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> OWNED_ENTRY_MEMORY_COST =
            newUpdater(LocalReplicatedMapStatsImpl.class, "ownedEntryMemoryCost");

    // these fields are only accessed through the updaters
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
    private volatile long totalGetLatencies;
    @Probe
    private volatile long totalPutLatencies;
    @Probe
    private volatile long totalRemoveLatencies;
    @Probe
    private volatile long maxGetLatency;
    @Probe
    private volatile long maxPutLatency;
    @Probe
    private volatile long maxRemoveLatency;

    @Probe
    private volatile long creationTime;
    @Probe
    private volatile long ownedEntryCount;
    @Probe
    private volatile long ownedEntryMemoryCost;

    public LocalReplicatedMapStatsImpl() {
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
        return 0;
    }

    // TODO: unused
    public void setBackupEntryCount(long backupEntryCount) {
    }

    @Override
    public int getBackupCount() {
        return 0;
    }

    // TODO: unused
    public void setBackupCount(int backupCount) {
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost;
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        OWNED_ENTRY_MEMORY_COST.set(this, ownedEntryMemoryCost);
    }

    @Override
    public long getBackupEntryMemoryCost() {
        return 0;
    }

    // TODO: unused
    public void setBackupEntryMemoryCost(long backupEntryMemoryCost) {
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
        HITS.set(this, hits);
    }

    @Override
    public long getLockedEntryCount() {
        return 0;
    }

    // TODO: unused
    public void setLockedEntryCount(long lockedEntryCount) {
    }

    @Override
    public long getDirtyEntryCount() {
        return 0;
    }

    // TODO: unused
    public void setDirtyEntryCount(long dirtyEntryCount) {
    }

    @Probe
    @Override
    public long total() {
        return putCount + getCount + removeCount + numberOfOtherOperations;
    }

    @Override
    public long getPutOperationCount() {
        return putCount;
    }

    public void incrementPuts(long latency) {
        PUT_COUNT.incrementAndGet(this);
        TOTAL_PUT_LATENCIES.addAndGet(this, latency);
        setMax(this, MAX_PUT_LATENCY, latency);
    }

    @Override
    public long getGetOperationCount() {
        return getCount;
    }

    public void incrementGets(long latency) {
        GET_COUNT.incrementAndGet(this);
        TOTAL_GET_LATENCIES.addAndGet(this, latency);
        setMax(this, MAX_GET_LATENCY, latency);
    }

    @Override
    public long getRemoveOperationCount() {
        return removeCount;
    }

    public void incrementRemoves(long latency) {
        REMOVE_COUNT.incrementAndGet(this);
        TOTAL_REMOVE_LATENCIES.addAndGet(this, latency);
        setMax(this, MAX_REMOVE_LATENCY, latency);
    }

    @Override
    public long getTotalPutLatency() {
        return totalPutLatencies;
    }

    @Override
    public long getTotalGetLatency() {
        return totalGetLatencies;
    }

    @Override
    public long getTotalRemoveLatency() {
        return totalRemoveLatencies;
    }

    @Override
    public long getMaxPutLatency() {
        return maxPutLatency;
    }

    @Override
    public long getMaxGetLatency() {
        return maxGetLatency;
    }

    @Override
    public long getMaxRemoveLatency() {
        return maxRemoveLatency;
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
        return 0;
    }

    // TODO: unused
    public void setHeapCost(long heapCost) {
    }

    @Override
    public long getMerkleTreesCost() {
        return 0;
    }

    // TODO: unused
    public void setMerkleTreesCost(long merkleTreesCost) {
    }

    @Override
    public NearCacheStatsImpl getNearCacheStats() {
        throw new UnsupportedOperationException("Replicated map has no Near Cache!");
    }

    @Override
    public long getQueryCount() {
        throw new UnsupportedOperationException("Queries on replicated maps are not supported.");
    }

    @Override
    public long getIndexedQueryCount() {
        throw new UnsupportedOperationException("Queries on replicated maps are not supported.");
    }

    @Override
    public Map<String, LocalIndexStats> getIndexStats() {
        throw new UnsupportedOperationException("Queries on replicated maps are not supported.");
    }

    @Override
    public long getSetOperationCount() {
        throw new UnsupportedOperationException("Set operation on replicated maps is not supported.");
    }

    @Override
    public long getTotalSetLatency() {
        throw new UnsupportedOperationException("Set operation on replicated maps is not supported.");
    }

    @Override
    public long getMaxSetLatency() {
        throw new UnsupportedOperationException("Set operation on replicated maps is not supported.");
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
        root.add("ownedEntryMemoryCost", ownedEntryMemoryCost);
        root.add("creationTime", creationTime);
        root.add("totalGetLatencies", totalGetLatencies);
        root.add("totalPutLatencies", totalPutLatencies);
        root.add("totalRemoveLatencies", totalRemoveLatencies);
        root.add("maxGetLatency", maxGetLatency);
        root.add("maxPutLatency", maxPutLatency);
        root.add("maxRemoveLatency", maxRemoveLatency);
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
        hits = getLong(json, "hits", -1L);
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        ownedEntryMemoryCost = getLong(json, "ownedEntryMemoryCost", -1L);
        creationTime = getLong(json, "creationTime", -1L);
        totalGetLatencies = getLong(json, "totalGetLatencies", -1L);
        totalPutLatencies = getLong(json, "totalPutLatencies", -1L);
        totalRemoveLatencies = getLong(json, "totalRemoveLatencies", -1L);
        maxGetLatency = getLong(json, "maxGetLatency", -1L);
        maxPutLatency = getLong(json, "maxPutLatency", -1L);
        maxRemoveLatency = getLong(json, "maxRemoveLatency", -1L);
    }

    @Override
    public String toString() {
        return "LocalReplicatedMapStatsImpl{"
                + "lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", hits=" + hits
                + ", numberOfOtherOperations=" + numberOfOtherOperations
                + ", numberOfEvents=" + numberOfEvents
                + ", getCount=" + getCount
                + ", putCount=" + putCount
                + ", removeCount=" + removeCount
                + ", totalGetLatencies=" + totalGetLatencies
                + ", totalPutLatencies=" + totalPutLatencies
                + ", totalRemoveLatencies=" + totalRemoveLatencies
                + ", ownedEntryCount=" + ownedEntryCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", creationTime=" + creationTime
                + '}';
    }
}
