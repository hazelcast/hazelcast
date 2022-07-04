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
import com.hazelcast.partition.LocalReplicationStats;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_MAX_GET_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_MAX_PUT_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_MAX_REMOVE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_GET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_HITS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_LAST_UPDATE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_NUMBER_OF_EVENTS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_NUMBER_OF_OTHER_OPERATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_PUT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_REMOVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_TOTAL_GET_LATENCIES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_TOTAL_PUT_LATENCIES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_METRIC_TOTAL_REMOVE_LATENCIES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_OWNED_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_OWNED_ENTRY_MEMORY_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_TOTAL;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;
import static com.hazelcast.internal.util.TimeUtil.convertNanosToMillis;
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
            newUpdater(LocalReplicatedMapStatsImpl.class, "totalGetLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> TOTAL_PUT_LATENCIES =
            newUpdater(LocalReplicatedMapStatsImpl.class, "totalPutLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> TOTAL_REMOVE_LATENCIES =
            newUpdater(LocalReplicatedMapStatsImpl.class, "totalRemoveLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_GET_LATENCY =
            newUpdater(LocalReplicatedMapStatsImpl.class, "maxGetLatencyNanos");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_PUT_LATENCY =
            newUpdater(LocalReplicatedMapStatsImpl.class, "maxPutLatencyNanos");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_REMOVE_LATENCY =
            newUpdater(LocalReplicatedMapStatsImpl.class, "maxRemoveLatencyNanos");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> OWNED_ENTRY_MEMORY_COST =
            newUpdater(LocalReplicatedMapStatsImpl.class, "ownedEntryMemoryCost");

    // these fields are only accessed through the updaters
    @Probe(name = REPLICATED_MAP_METRIC_LAST_ACCESS_TIME, unit = MS)
    private volatile long lastAccessTime;
    @Probe(name = REPLICATED_MAP_METRIC_LAST_UPDATE_TIME, unit = MS)
    private volatile long lastUpdateTime;
    @Probe(name = REPLICATED_MAP_METRIC_HITS)
    private volatile long hits;
    @Probe(name = REPLICATED_MAP_METRIC_NUMBER_OF_OTHER_OPERATIONS)
    private volatile long numberOfOtherOperations;
    @Probe(name = REPLICATED_MAP_METRIC_NUMBER_OF_EVENTS)
    private volatile long numberOfEvents;
    @Probe(name = REPLICATED_MAP_METRIC_GET_COUNT)
    private volatile long getCount;
    @Probe(name = REPLICATED_MAP_METRIC_PUT_COUNT)
    private volatile long putCount;
    @Probe(name = REPLICATED_MAP_METRIC_REMOVE_COUNT)
    private volatile long removeCount;

    private volatile long totalGetLatenciesNanos;
    private volatile long totalPutLatenciesNanos;
    private volatile long totalRemoveLatenciesNanos;
    private volatile long maxGetLatencyNanos;
    private volatile long maxPutLatencyNanos;
    private volatile long maxRemoveLatencyNanos;

    @Probe(name = REPLICATED_MAP_CREATION_TIME, unit = MS)
    private final long creationTime;
    @Probe(name = REPLICATED_MAP_OWNED_ENTRY_COUNT)
    private volatile long ownedEntryCount;
    @Probe(name = REPLICATED_MAP_OWNED_ENTRY_MEMORY_COST, unit = BYTES)
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

    @Probe(name = REPLICATED_MAP_TOTAL)
    @Override
    public long total() {
        return putCount + getCount + removeCount + numberOfOtherOperations;
    }

    @Override
    public long getPutOperationCount() {
        return putCount;
    }

    public void incrementPutsNanos(long latencyNanos) {
        PUT_COUNT.incrementAndGet(this);
        TOTAL_PUT_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_PUT_LATENCY, latencyNanos);
    }

    @Override
    public long getGetOperationCount() {
        return getCount;
    }

    public void incrementGetsNanos(long latencyNanos) {
        GET_COUNT.incrementAndGet(this);
        TOTAL_GET_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_GET_LATENCY, latencyNanos);
    }

    @Override
    public long getRemoveOperationCount() {
        return removeCount;
    }

    public void incrementRemovesNanos(long latencyNanos) {
        REMOVE_COUNT.incrementAndGet(this);
        TOTAL_REMOVE_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_REMOVE_LATENCY, latencyNanos);
    }

    @Probe(name = REPLICATED_MAP_METRIC_TOTAL_PUT_LATENCIES, unit = MS)
    @Override
    public long getTotalPutLatency() {
        return convertNanosToMillis(totalPutLatenciesNanos);
    }

    @Probe(name = REPLICATED_MAP_METRIC_TOTAL_GET_LATENCIES, unit = MS)
    @Override
    public long getTotalGetLatency() {
        return convertNanosToMillis(totalGetLatenciesNanos);
    }

    @Probe(name = REPLICATED_MAP_METRIC_TOTAL_REMOVE_LATENCIES, unit = MS)
    @Override
    public long getTotalRemoveLatency() {
        return convertNanosToMillis(totalRemoveLatenciesNanos);
    }

    @Probe(name = REPLICATED_MAP_MAX_PUT_LATENCY, unit = MS)
    @Override
    public long getMaxPutLatency() {
        return convertNanosToMillis(maxPutLatencyNanos);
    }

    @Probe(name = REPLICATED_MAP_MAX_GET_LATENCY, unit = MS)
    @Override
    public long getMaxGetLatency() {
        return convertNanosToMillis(maxGetLatencyNanos);
    }

    @Probe(name = REPLICATED_MAP_MAX_REMOVE_LATENCY, unit = MS)
    @Override
    public long getMaxRemoveLatency() {
        return convertNanosToMillis(maxRemoveLatencyNanos);
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
    public LocalReplicationStats getReplicationStats() {
        throw new UnsupportedOperationException("Replication stats are not available for replicated maps.");
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
                + ", totalGetLatencies=" + convertNanosToMillis(totalGetLatenciesNanos)
                + ", totalPutLatencies=" + convertNanosToMillis(totalPutLatenciesNanos)
                + ", totalRemoveLatencies=" + convertNanosToMillis(totalRemoveLatenciesNanos)
                + ", ownedEntryCount=" + ownedEntryCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", creationTime=" + creationTime
                + '}';
    }
}
