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

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexHeapMemoryCostUtil;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * The implementation of internal index stats specialized for global indexes.
 * <p>
 * The main trait of the implementation is the concurrency support, which is
 * required for global indexes because they are shared among partitions.
 */
public class GlobalIndexStats implements InternalIndexStats {

    // To compute the average hit cardinality we need to track the total
    // cardinality of all of the hits and then divide it by the number of hits.
    // But since the number of the indexed entries may change with time, this
    // raw total cardinality value can't provide any useful information. So
    // instead we store the total normalized cardinality, which sums up the
    // individual hit cardinalities divided by the index size at the time of the
    // hit. This total normalized cardinality is a floating point number with
    // which we can't work easily: we need to perform atomic operations on it
    // since global index stats are shared among all map partitions, but there
    // is no notion of atomic operations for doubles/floats. To overcome this
    // problem we store a scaled long representation which gives around 1%
    // precision.
    private static final long PRECISION_SCALE = 7;
    private static final long PRECISION = 1 << PRECISION_SCALE;

    private static final AtomicLongFieldUpdater<GlobalIndexStats> ENTRY_COUNT = newUpdater(GlobalIndexStats.class, "entryCount");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> QUERY_COUNT = newUpdater(GlobalIndexStats.class, "queryCount");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> HIT_COUNT = newUpdater(GlobalIndexStats.class, "hitCount");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> TOTAL_HIT_LATENCY = newUpdater(GlobalIndexStats.class,
            "totalHitLatency");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> TOTAL_NORMALIZED_HIT_CARDINALITY = newUpdater(
            GlobalIndexStats.class, "totalNormalizedHitCardinality");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> INSERT_COUNT = newUpdater(GlobalIndexStats.class,
            "insertCount");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> TOTAL_INSERT_LATENCY = newUpdater(GlobalIndexStats.class,
            "totalInsertLatency");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> UPDATE_COUNT = newUpdater(GlobalIndexStats.class,
            "updateCount");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> TOTAL_UPDATE_LATENCY = newUpdater(GlobalIndexStats.class,
            "totalUpdateLatency");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> REMOVE_COUNT = newUpdater(GlobalIndexStats.class,
            "removeCount");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> TOTAL_REMOVE_LATENCY = newUpdater(GlobalIndexStats.class,
            "totalRemoveLatency");
    private static final AtomicLongFieldUpdater<GlobalIndexStats> VALUES_MEMORY_COST = newUpdater(GlobalIndexStats.class,
            "valuesMemoryCost");

    private final boolean ordered;
    private final boolean queryableEntriesAreCached;
    private final long creationTime;

    private volatile long entryCount;
    private volatile long queryCount;
    private volatile long hitCount;
    private volatile long totalHitLatency;
    private volatile long totalNormalizedHitCardinality;
    private volatile long insertCount;
    private volatile long totalInsertLatency;
    private volatile long updateCount;
    private volatile long totalUpdateLatency;
    private volatile long removeCount;
    private volatile long totalRemoveLatency;
    private volatile long valuesMemoryCost;

    /**
     * Constructs a new instance of global index stats.
     *
     * @param ordered                   {@code true} if the stats are being created
     *                                  for an ordered index, {@code false} otherwise.
     *                                  Affects the on-heap memory cost calculation.
     * @param queryableEntriesAreCached {@code true} if the stats are being created
     *                                  for an index for which queryable entries are
     *                                  cached, {@code false} otherwise. Affects the
     *                                  on-heap memory cost calculation.
     */
    public GlobalIndexStats(boolean ordered, boolean queryableEntriesAreCached) {
        this.ordered = ordered;
        this.queryableEntriesAreCached = queryableEntriesAreCached;
        this.creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long makeTimestamp() {
        return System.nanoTime();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    @Override
    public void incrementQueryCount() {
        QUERY_COUNT.incrementAndGet(this);
    }

    @Override
    public long getHitCount() {
        return hitCount;
    }

    @Override
    public long getTotalHitLatency() {
        return totalHitLatency;
    }

    @Override
    public double getTotalNormalizedHitCardinality() {
        return (double) totalNormalizedHitCardinality / PRECISION;
    }

    @Override
    public long getInsertCount() {
        return insertCount;
    }

    @Override
    public long getTotalInsertLatency() {
        return totalInsertLatency;
    }

    @Override
    public long getUpdateCount() {
        return updateCount;
    }

    @Override
    public long getTotalUpdateLatency() {
        return totalUpdateLatency;
    }

    @Override
    public long getRemoveCount() {
        return removeCount;
    }

    @Override
    public long getTotalRemoveLatency() {
        return totalRemoveLatency;
    }

    @Override
    public long getOnHeapMemoryCost() {
        return IndexHeapMemoryCostUtil.estimateMapCost(entryCount, ordered, queryableEntriesAreCached) + valuesMemoryCost;
    }

    @Override
    public long getOffHeapMemoryCost() {
        return 0;
    }

    @Override
    public void onInsert(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource) {
        // XXX: AddIndexOperation may be invoked at any time by a user as a
        // result of IMap.addIndex call and we can't tell for sure are we
        // building a new index or rebuilding the existing one, so we are just
        // ignoring the attempts to reinsert the entries.
        if (operationStats.getEntryCountDelta() == 0) {
            return;
        }

        if (operationSource == Index.OperationSource.User) {
            TOTAL_INSERT_LATENCY.addAndGet(this, System.nanoTime() - timestamp);
            INSERT_COUNT.incrementAndGet(this);
        }
        ENTRY_COUNT.addAndGet(this, operationStats.getEntryCountDelta());
        VALUES_MEMORY_COST.addAndGet(this, operationStats.getMemoryCostDelta());
    }

    @Override
    public void onUpdate(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource) {
        if (operationSource == Index.OperationSource.User) {
            TOTAL_UPDATE_LATENCY.addAndGet(this, System.nanoTime() - timestamp);
            UPDATE_COUNT.incrementAndGet(this);
        }
        ENTRY_COUNT.addAndGet(this, operationStats.getEntryCountDelta());
        VALUES_MEMORY_COST.addAndGet(this, operationStats.getMemoryCostDelta());
    }

    @Override
    public void onRemove(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource) {
        if (operationSource == Index.OperationSource.User) {
            TOTAL_REMOVE_LATENCY.addAndGet(this, System.nanoTime() - timestamp);
            REMOVE_COUNT.incrementAndGet(this);
        }
        ENTRY_COUNT.addAndGet(this, operationStats.getEntryCountDelta());
        VALUES_MEMORY_COST.addAndGet(this, operationStats.getMemoryCostDelta());
    }

    @Override
    public void onClear() {
        entryCount = 0;
        valuesMemoryCost = 0;
    }

    @Override
    public void onIndexHit(long timestamp, long hitCardinality) {
        long localEntryCount = entryCount;
        if (localEntryCount == 0) {
            // selecting nothing from nothing is not counted as a hit
            return;
        }

        TOTAL_HIT_LATENCY.addAndGet(this, System.nanoTime() - timestamp);
        HIT_COUNT.incrementAndGet(this);

        // limit the cardinality for "safety"
        long adjustedHitCardinality = Math.min(hitCardinality, localEntryCount);

        // scale the cardinality to maintain the precision
        long scaledHitCardinality = adjustedHitCardinality << PRECISION_SCALE;

        // this will produce a value in [0, PRECISION] range
        long normalizedHitCardinality = scaledHitCardinality / localEntryCount;

        TOTAL_NORMALIZED_HIT_CARDINALITY.addAndGet(this, normalizedHitCardinality);
    }

    @Override
    public void resetPerQueryStats() {
        // Do nothing, per-query stats are tracked in GlobalQueryContextWithStats
        // since this stats instance is shared among queries.
    }

    @Override
    public MemoryAllocator wrapMemoryAllocator(MemoryAllocator memoryAllocator) {
        throw new UnsupportedOperationException("global indexes are not supposed to use native memory allocators");
    }

    @Override
    public IndexOperationStats createOperationStats() {
        return new GlobalIndexOperationStats();
    }

}
