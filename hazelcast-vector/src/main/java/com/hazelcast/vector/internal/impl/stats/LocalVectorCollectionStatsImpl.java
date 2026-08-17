/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.stats;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.tpcengine.util.ReflectionUtil;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.JVMUtil;

import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_BACKUP_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_BACKUP_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_BACKUP_ENTRY_HEAP_MEMORY_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_CLEAR_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_DELETE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_GET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_HEAP_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_LAST_UPDATE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_MAX_CLEAR_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_MAX_OPTIMIZE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_MAX_SEARCH_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_MAX_SIZE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_OPTIMIZE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_OWNED_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_OWNED_ENTRY_HEAP_MEMORY_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_PUT_ALL_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_PUT_ALL_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_PUT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_REMOVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SEARCH_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SEARCH_INDEX_QUERY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SEARCH_INDEX_VISITED_NODES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SEARCH_RESULTS_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SIZE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_CLEAR_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_DELETE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_GET_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_MAX_DELETE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_MAX_GET_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_MAX_PUT_ALL_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_MAX_PUT_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_MAX_REMOVE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_MAX_SET_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_OPTIMIZE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_PUT_ALL_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_PUT_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_REMOVE_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_SEARCH_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_SET_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_TOTAL_SIZE_LATENCY;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;
import static com.hazelcast.internal.util.TimeUtil.convertNanosToMillis;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

@SuppressWarnings("MethodCount")
public class LocalVectorCollectionStatsImpl implements LocalVectorCollectionStats {
    static final long FIXED_HEAP_BYTES_USED = JVMUtil.OBJECT_HEADER_SIZE + Integer.BYTES + 38 * Long.BYTES;

    private static final VarHandle LAST_ACCESS_TIME = ReflectionUtil.findVarHandle("lastAccessTime", long.class);
    private static final VarHandle LAST_UPDATE_TIME = ReflectionUtil.findVarHandle("lastUpdateTime", long.class);

    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> GET_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "getCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> PUT_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "putCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> PUT_ALL_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "putAllCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> PUT_ALL_ENTRY_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "putAllEntryCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> SET_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "setCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> REMOVE_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "removeCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> DELETE_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "deleteCount");

    // The resolution is in nanoseconds for the following latencies
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_GET_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalGetLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_PUT_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalPutLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_PUT_ALL_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalPutAllLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_SET_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalSetLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_REMOVE_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalRemoveLatenciesNanos");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_DELETE_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalDeleteLatenciesNanos");
    private static final VarHandle MAX_GET_LATENCY = ReflectionUtil.findVarHandle("maxGetLatency", long.class);
    private static final VarHandle MAX_PUT_LATENCY = ReflectionUtil.findVarHandle("maxPutLatency", long.class);
    private static final VarHandle MAX_PUT_ALL_LATENCY = ReflectionUtil.findVarHandle("maxPutAllLatency", long.class);
    private static final VarHandle MAX_SET_LATENCY = ReflectionUtil.findVarHandle("maxSetLatency", long.class);
    private static final VarHandle MAX_REMOVE_LATENCY = ReflectionUtil.findVarHandle("maxRemoveLatency", long.class);
    private static final VarHandle MAX_DELETE_LATENCY = ReflectionUtil.findVarHandle("maxDeleteLatency", long.class);

    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> SEARCH_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "searchCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> SEARCH_RESULTS_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "searchResultsCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_SEARCH_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalSearchLatenciesNanos");
    private static final VarHandle MAX_SEARCH_LATENCY = ReflectionUtil.findVarHandle("maxSearchLatency", long.class);

    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> OPTIMIZE_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "optimizeCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_OPTIMIZE_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalOptimizeLatenciesNanos");
    private static final VarHandle MAX_OPTIMIZE_LATENCY = ReflectionUtil.findVarHandle("maxOptimizeLatency", long.class);

    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> CLEAR_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "clearCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_CLEAR_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalClearLatenciesNanos");
    private static final VarHandle MAX_CLEAR_LATENCY = ReflectionUtil.findVarHandle("maxClearLatency", long.class);

    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> SIZE_COUNT =
            newUpdater(LocalVectorCollectionStatsImpl.class, "sizeCount");
    private static final AtomicLongFieldUpdater<LocalVectorCollectionStatsImpl> TOTAL_SIZE_LATENCIES =
            newUpdater(LocalVectorCollectionStatsImpl.class, "totalSizeLatenciesNanos");
    private static final VarHandle MAX_SIZE_LATENCY = ReflectionUtil.findVarHandle("maxSizeLatency", long.class);

    // These fields are only accessed through the updaters
    private volatile long lastAccessTime;
    @Probe(name = VECTOR_COLLECTION_LAST_UPDATE_TIME, unit = MS)
    private volatile long lastUpdateTime;

    @Probe(name = VECTOR_COLLECTION_GET_COUNT)
    private volatile long getCount;
    @Probe(name = VECTOR_COLLECTION_PUT_COUNT)
    private volatile long putCount;
    @Probe(name = VECTOR_COLLECTION_PUT_ALL_COUNT)
    private volatile long putAllCount;
    @Probe(name = VECTOR_COLLECTION_PUT_ALL_ENTRY_COUNT)
    private volatile long putAllEntryCount;
    @Probe(name = VECTOR_COLLECTION_SET_COUNT)
    private volatile long setCount;
    @Probe(name = VECTOR_COLLECTION_REMOVE_COUNT)
    private volatile long removeCount;
    @Probe(name = VECTOR_COLLECTION_DELETE_COUNT)
    private volatile long deleteCount;

    private volatile long totalGetLatenciesNanos;
    private volatile long totalPutLatenciesNanos;
    private volatile long totalPutAllLatenciesNanos;
    private volatile long totalSetLatenciesNanos;
    private volatile long totalRemoveLatenciesNanos;
    private volatile long totalDeleteLatenciesNanos;

    private volatile long maxGetLatency;
    private volatile long maxPutLatency;
    private volatile long maxPutAllLatency;
    private volatile long maxSetLatency;
    private volatile long maxRemoveLatency;
    private volatile long maxDeleteLatency;

    @Probe(name = VECTOR_COLLECTION_SEARCH_COUNT)
    private volatile long searchCount;
    @Probe(name = VECTOR_COLLECTION_SEARCH_RESULTS_COUNT)
    private volatile long searchResultsCount;
    private volatile long totalSearchLatenciesNanos;
    private volatile long maxSearchLatency;

    @Probe(name = VECTOR_COLLECTION_OPTIMIZE_COUNT)
    private volatile long optimizeCount;
    private volatile long totalOptimizeLatenciesNanos;
    private volatile long maxOptimizeLatency;

    @Probe(name = VECTOR_COLLECTION_CLEAR_COUNT)
    private volatile long clearCount;
    private volatile long totalClearLatenciesNanos;
    private volatile long maxClearLatency;

    @Probe(name = VECTOR_COLLECTION_SIZE_COUNT)
    private volatile long sizeCount;
    private volatile long totalSizeLatenciesNanos;
    private volatile long maxSizeLatency;

    @Probe(name = VECTOR_COLLECTION_CREATION_TIME, unit = MS)
    private final long creationTime;

    ///// on demand stats
    @Probe(name = VECTOR_COLLECTION_OWNED_ENTRY_COUNT)
    private volatile long ownedEntryCount;
    @Probe(name = VECTOR_COLLECTION_BACKUP_ENTRY_COUNT)
    private volatile long backupEntryCount;
    @Probe(name = VECTOR_COLLECTION_OWNED_ENTRY_HEAP_MEMORY_COST, unit = BYTES)
    private volatile long ownedEntryHeapMemoryCost;
    @Probe(name = VECTOR_COLLECTION_BACKUP_ENTRY_HEAP_MEMORY_COST, unit = BYTES)
    private volatile long backupEntryHeapMemoryCost;

    /**
     * Holds total heap cost
     */
    @Probe(name = VECTOR_COLLECTION_HEAP_COST, unit = BYTES)
    private volatile long heapCost;

    @Probe(name = VECTOR_COLLECTION_BACKUP_COUNT)
    private volatile int backupCount;

    /**
     * Index stats aggregated on a collection level, from all indexes and partitions
     */
    private final VectorIndexStatsImpl vectorIndexStats = new VectorIndexStatsImpl();

    LocalVectorCollectionStatsImpl() {
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
    public long getOwnedEntryHeapMemoryCost() {
        return ownedEntryHeapMemoryCost;
    }

    public void setOwnedEntryHeapMemoryCost(long ownedEntryHeapMemoryCost) {
        this.ownedEntryHeapMemoryCost = ownedEntryHeapMemoryCost;
    }

    @Override
    public long getBackupEntryHeapMemoryCost() {
        return backupEntryHeapMemoryCost;
    }

    public void setBackupEntryHeapMemoryCost(long backupEntryHeapMemoryCost) {
        this.backupEntryHeapMemoryCost = backupEntryHeapMemoryCost;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    @Probe(name = VECTOR_COLLECTION_LAST_ACCESS_TIME, unit = MS)
    public long getLastAccessTime() {
        // updates change only lastUpdateTime but count as access
        return Math.max(lastAccessTime, lastUpdateTime);
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

    @Override
    public long getDeleteOperationCount() {
        return deleteCount;
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_PUT_LATENCY, unit = MS)
    @Override
    public long getTotalPutLatency() {
        return convertNanosToMillis(totalPutLatenciesNanos);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_PUT_ALL_LATENCY, unit = MS)
    @Override
    public long getTotalPutAllLatency() {
        return convertNanosToMillis(totalPutAllLatenciesNanos);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_SET_LATENCY, unit = MS)
    @Override
    public long getTotalSetLatency() {
        return convertNanosToMillis(totalSetLatenciesNanos);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_GET_LATENCY, unit = MS)
    @Override
    public long getTotalGetLatency() {
        return convertNanosToMillis(totalGetLatenciesNanos);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_REMOVE_LATENCY, unit = MS)
    @Override
    public long getTotalRemoveLatency() {
        return convertNanosToMillis(totalRemoveLatenciesNanos);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_DELETE_LATENCY, unit = MS)
    @Override
    public long getTotalDeleteLatency() {
        return convertNanosToMillis(totalDeleteLatenciesNanos);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_MAX_PUT_LATENCY, unit = MS)
    @Override
    public long getMaxPutLatency() {
        return convertNanosToMillis(maxPutLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_MAX_PUT_ALL_LATENCY, unit = MS)
    @Override
    public long getMaxPutAllLatency() {
        return convertNanosToMillis(maxPutAllLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_MAX_SET_LATENCY, unit = MS)
    @Override
    public long getMaxSetLatency() {
        return convertNanosToMillis(maxSetLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_MAX_GET_LATENCY, unit = MS)
    @Override
    public long getMaxGetLatency() {
        return convertNanosToMillis(maxGetLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_MAX_REMOVE_LATENCY, unit = MS)
    @Override
    public long getMaxRemoveLatency() {
        return convertNanosToMillis(maxRemoveLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_MAX_DELETE_LATENCY, unit = MS)
    @Override
    public long getMaxDeleteLatency() {
        return convertNanosToMillis(maxDeleteLatency);
    }

    @Override
    public long getHeapCost() {
        return heapCost;
    }

    public void setHeapCost(long heapCost) {
        this.heapCost = heapCost;
    }

    public void incrementPutLatencyNanos(long latencyNanos) {
        PUT_COUNT.incrementAndGet(this);
        TOTAL_PUT_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_PUT_LATENCY, latencyNanos);
        onUpdate();
    }

    public void incrementPutAllLatencyNanos(long delta, long latencyNanos) {
        PUT_ALL_COUNT.incrementAndGet(this);
        PUT_ALL_ENTRY_COUNT.addAndGet(this, delta);
        TOTAL_PUT_ALL_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_PUT_ALL_LATENCY, latencyNanos);
        onUpdate();
    }

    public void incrementSetLatencyNanos(long latencyNanos) {
        SET_COUNT.incrementAndGet(this);
        TOTAL_SET_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_SET_LATENCY, latencyNanos);
        onUpdate();
    }

    public void incrementGetLatencyNanos(long latencyNanos) {
        GET_COUNT.incrementAndGet(this);
        TOTAL_GET_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_GET_LATENCY, latencyNanos);
        onAccess();
    }

    public void incrementRemoveLatencyNanos(long latencyNanos) {
        REMOVE_COUNT.incrementAndGet(this);
        TOTAL_REMOVE_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_REMOVE_LATENCY, latencyNanos);
        onUpdate();
    }

    public void incrementDeleteLatencyNanos(long latencyNanos) {
        DELETE_COUNT.incrementAndGet(this);
        TOTAL_DELETE_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_DELETE_LATENCY, latencyNanos);
        onUpdate();
    }

    @Override
    public long getSearchCount() {
        return searchCount;
    }

    @Override
    public long getSearchResultsCount() {
        return searchResultsCount;
    }

    @Probe(name = VECTOR_COLLECTION_MAX_SEARCH_LATENCY, unit = MS)
    @Override
    public long getMaxSearchLatency() {
        return convertNanosToMillis(maxSearchLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_SEARCH_LATENCY, unit = MS)
    @Override
    public long getTotalSearchLatency() {
        return convertNanosToMillis(totalSearchLatenciesNanos);
    }

    public void incrementSearchLatencyNanos(long resultsCount, long latencyNanos) {
        SEARCH_COUNT.incrementAndGet(this);
        SEARCH_RESULTS_COUNT.addAndGet(this, resultsCount);
        TOTAL_SEARCH_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_SEARCH_LATENCY, latencyNanos);
        onAccess();
    }

    @Probe(name = VECTOR_COLLECTION_SEARCH_INDEX_QUERY_COUNT, unit = COUNT)
    @Override
    public long getSearchIndexQueryCount() {
        return vectorIndexStats.getQueryCount();
    }

    @Probe(name = VECTOR_COLLECTION_SEARCH_INDEX_VISITED_NODES, unit = COUNT)
    @Override
    public long getSearchIndexVisitedNodes() {
        return vectorIndexStats.getVisitedNodes();
    }

    public void setVectorIndexStats(VectorIndexStats vectorIndexStats) {
        this.vectorIndexStats.set(vectorIndexStats);
    }

    @Override
    public long getOptimizeCount() {
        return optimizeCount;
    }

    @Probe(name = VECTOR_COLLECTION_MAX_OPTIMIZE_LATENCY, unit = MS)
    @Override
    public long getMaxOptimizeLatency() {
        return convertNanosToMillis(maxOptimizeLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_OPTIMIZE_LATENCY, unit = MS)
    @Override
    public long getTotalOptimizeLatency() {
        return convertNanosToMillis(totalOptimizeLatenciesNanos);
    }

    public void incrementOptimizeLatencyNanos(long latencyNanos) {
        OPTIMIZE_COUNT.incrementAndGet(this);
        TOTAL_OPTIMIZE_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_OPTIMIZE_LATENCY, latencyNanos);
        onAccess();
    }


    @Override
    public long getClearCount() {
        return clearCount;
    }

    @Probe(name = VECTOR_COLLECTION_MAX_CLEAR_LATENCY, unit = MS)
    @Override
    public long getMaxClearLatency() {
        return convertNanosToMillis(maxClearLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_CLEAR_LATENCY, unit = MS)
    @Override
    public long getTotalClearLatency() {
        return convertNanosToMillis(totalClearLatenciesNanos);
    }

    public void incrementClearLatencyNanos(long latencyNanos) {
        CLEAR_COUNT.incrementAndGet(this);
        TOTAL_CLEAR_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_CLEAR_LATENCY, latencyNanos);
        onUpdate();
    }


    @Override
    public long getSizeCount() {
        return sizeCount;
    }

    @Probe(name = VECTOR_COLLECTION_MAX_SIZE_LATENCY, unit = MS)
    @Override
    public long getMaxSizeLatency() {
        return convertNanosToMillis(maxSizeLatency);
    }

    @Probe(name = VECTOR_COLLECTION_TOTAL_SIZE_LATENCY, unit = MS)
    @Override
    public long getTotalSizeLatency() {
        return convertNanosToMillis(totalSizeLatenciesNanos);
    }

    public void incrementSizeLatencyNanos(long latencyNanos) {
        SIZE_COUNT.incrementAndGet(this);
        TOTAL_SIZE_LATENCIES.addAndGet(this, latencyNanos);
        setMax(this, MAX_SIZE_LATENCY, latencyNanos);
        onAccess();
    }


    private void onAccess() {
        setLastAccessTime(Clock.currentTimeMillis());
    }

    private void onUpdate() {
        setLastUpdateTime(Clock.currentTimeMillis());
    }
}
