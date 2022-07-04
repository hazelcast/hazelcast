/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.query.LocalIndexStats;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_BACKUP_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_BACKUP_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_BACKUP_ENTRY_MEMORY_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_DIRTY_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_GET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_HEAP_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_HITS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_INDEXED_QUERY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_LAST_UPDATE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_LOCKED_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_MERKLE_TREES_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_NUMBER_OF_EVENTS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_NUMBER_OF_OTHER_OPERATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_OWNED_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_OWNED_ENTRY_MEMORY_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_PUT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_QUERY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_REMOVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_SET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_TOTAL_GET_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_TOTAL_MAX_GET_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_TOTAL_MAX_PUT_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_TOTAL_MAX_REMOVE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_TOTAL_MAX_SET_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_TOTAL_PUT_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_TOTAL_REMOVE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_TOTAL_SET_LATENCY;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;
import static com.hazelcast.internal.util.TimeUtil.convertNanosToMillis;
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
            new ConcurrentHashMap<>();
    private final Map<String, LocalIndexStats> indexStats = Collections.unmodifiableMap(mutableIndexStats);
    private final LocalReplicationStatsImpl replicationStats = new LocalReplicationStatsImpl();

    // These fields are only accessed through the updaters
    @Probe(name = MAP_METRIC_LAST_ACCESS_TIME, unit = MS)
    private volatile long lastAccessTime;
    @Probe(name = MAP_METRIC_LAST_UPDATE_TIME, unit = MS)
    private volatile long lastUpdateTime;
    @Probe(name = MAP_METRIC_HITS)
    private volatile long hits;
    @Probe(name = MAP_METRIC_NUMBER_OF_OTHER_OPERATIONS)
    private volatile long numberOfOtherOperations;
    @Probe(name = MAP_METRIC_NUMBER_OF_EVENTS)
    private volatile long numberOfEvents;
    @Probe(name = MAP_METRIC_GET_COUNT)
    private volatile long getCount;
    @Probe(name = MAP_METRIC_PUT_COUNT)
    private volatile long putCount;
    @Probe(name = MAP_METRIC_SET_COUNT)
    private volatile long setCount;
    @Probe(name = MAP_METRIC_REMOVE_COUNT)
    private volatile long removeCount;
    private volatile long totalGetLatenciesNanos;
    private volatile long totalPutLatenciesNanos;
    private volatile long totalSetLatenciesNanos;
    private volatile long totalRemoveLatenciesNanos;
    private volatile long maxGetLatency;
    private volatile long maxPutLatency;
    private volatile long maxSetLatency;
    private volatile long maxRemoveLatency;
    @Probe(name = MAP_METRIC_CREATION_TIME, unit = MS)
    private final long creationTime;
    @Probe(name = MAP_METRIC_OWNED_ENTRY_COUNT)
    private volatile long ownedEntryCount;
    @Probe(name = MAP_METRIC_BACKUP_ENTRY_COUNT)
    private volatile long backupEntryCount;
    @Probe(name = MAP_METRIC_OWNED_ENTRY_MEMORY_COST, unit = BYTES)
    private volatile long ownedEntryMemoryCost;
    @Probe(name = MAP_METRIC_BACKUP_ENTRY_MEMORY_COST, unit = BYTES)
    private volatile long backupEntryMemoryCost;
    /**
     * Holds total heap cost of map & Near Cache & backups & Merkle trees.
     */
    @Probe(name = MAP_METRIC_HEAP_COST)
    private volatile long heapCost;
    /**
     * Holds the total memory footprint of the Merkle trees
     */
    @Probe(name = MAP_METRIC_MERKLE_TREES_COST)
    private volatile long merkleTreesCost;
    @Probe(name = MAP_METRIC_LOCKED_ENTRY_COUNT)
    private volatile long lockedEntryCount;
    @Probe(name = MAP_METRIC_DIRTY_ENTRY_COUNT)
    private volatile long dirtyEntryCount;
    @Probe(name = MAP_METRIC_BACKUP_COUNT)
    private volatile int backupCount;
    private volatile NearCacheStats nearCacheStats;
    @Probe(name = MAP_METRIC_QUERY_COUNT)
    private volatile long queryCount;
    @Probe(name = MAP_METRIC_INDEXED_QUERY_COUNT)
    private volatile long indexedQueryCount;

    private final boolean ignoreMemoryCosts;

    public LocalMapStatsImpl(boolean ignoreMemoryCosts) {
        this.ignoreMemoryCosts = ignoreMemoryCosts;
        creationTime = Clock.currentTimeMillis();
        if (ignoreMemoryCosts) {
            ownedEntryMemoryCost = -1;
            backupEntryMemoryCost = -1;
        }
    }

    public LocalMapStatsImpl() {
        this(false);
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
        if (!ignoreMemoryCosts) {
            this.ownedEntryMemoryCost = ownedEntryMemoryCost;
        }
    }

    @Override
    public long getBackupEntryMemoryCost() {
        return backupEntryMemoryCost;
    }

    public void setBackupEntryMemoryCost(long backupEntryMemoryCost) {
        if (!ignoreMemoryCosts) {
            this.backupEntryMemoryCost = backupEntryMemoryCost;
        }
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

    @Probe(name = MAP_METRIC_TOTAL_PUT_LATENCY, unit = MS)
    @Override
    public long getTotalPutLatency() {
        return convertNanosToMillis(totalPutLatenciesNanos);
    }

    @Probe(name = MAP_METRIC_TOTAL_SET_LATENCY, unit = MS)
    @Override
    public long getTotalSetLatency() {
        return convertNanosToMillis(totalSetLatenciesNanos);
    }

    @Probe(name = MAP_METRIC_TOTAL_GET_LATENCY, unit = MS)
    @Override
    public long getTotalGetLatency() {
        return convertNanosToMillis(totalGetLatenciesNanos);
    }

    @Probe(name = MAP_METRIC_TOTAL_REMOVE_LATENCY, unit = MS)
    @Override
    public long getTotalRemoveLatency() {
        return convertNanosToMillis(totalRemoveLatenciesNanos);
    }

    @Probe(name = MAP_METRIC_TOTAL_MAX_PUT_LATENCY, unit = MS)
    @Override
    public long getMaxPutLatency() {
        return convertNanosToMillis(maxPutLatency);
    }

    @Probe(name = MAP_METRIC_TOTAL_MAX_SET_LATENCY, unit = MS)
    @Override
    public long getMaxSetLatency() {
        return convertNanosToMillis(maxSetLatency);
    }

    @Probe(name = MAP_METRIC_TOTAL_MAX_GET_LATENCY, unit = MS)
    @Override
    public long getMaxGetLatency() {
        return convertNanosToMillis(maxGetLatency);
    }

    @Probe(name = MAP_METRIC_TOTAL_MAX_REMOVE_LATENCY, unit = MS)
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

    @Override
    public LocalReplicationStatsImpl getReplicationStats() {
        return replicationStats;
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
                + ", replicationStats=" + replicationStats
                + '}';
    }
}
