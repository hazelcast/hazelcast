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

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.query.impl.Index;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * The implementation of internal index stats specialized for partitioned indexes.
 * <p>
 * The main trait of the implementation is the lack of concurrency support since
 * partitioned indexes and their stats are updated from a single thread only.
 */
public class PartitionPerIndexStats implements PerIndexStats {

    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> ENTRY_COUNT = newUpdater(PartitionPerIndexStats.class,
            "entryCount");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> QUERY_COUNT = newUpdater(PartitionPerIndexStats.class,
            "queryCount");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> HIT_COUNT = newUpdater(PartitionPerIndexStats.class,
            "hitCount");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> TOTAL_HIT_LATENCY = newUpdater(
            PartitionPerIndexStats.class, "totalHitLatency");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> TOTAL_NORMALIZED_HIT_CARDINALITY = newUpdater(
            PartitionPerIndexStats.class, "totalNormalizedHitCardinality");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> INSERT_COUNT = newUpdater(PartitionPerIndexStats.class,
            "insertCount");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> TOTAL_INSERT_LATENCY = newUpdater(
            PartitionPerIndexStats.class, "totalInsertLatency");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> UPDATE_COUNT = newUpdater(PartitionPerIndexStats.class,
            "updateCount");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> TOTAL_UPDATE_LATENCY = newUpdater(
            PartitionPerIndexStats.class, "totalUpdateLatency");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> REMOVE_COUNT = newUpdater(PartitionPerIndexStats.class,
            "removeCount");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> TOTAL_REMOVE_LATENCY = newUpdater(
            PartitionPerIndexStats.class, "totalRemoveLatency");
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> MEMORY_COST = newUpdater(PartitionPerIndexStats.class,
            "memoryCost");

    // Per-operation stats may be safely reused/shared for operations on
    // partitioned indexes since we know for sure only a single thread may
    // mutate the index.
    private final PartitionIndexOperationStats operationStats = new PartitionIndexOperationStats();

    private final long creationTime;

    private volatile long entryCount;
    private volatile long queryCount;
    private volatile long hitCount;
    private volatile long totalHitLatency;
    private volatile long totalNormalizedHitCardinality = Double.doubleToRawLongBits(0.0);
    private volatile long insertCount;
    private volatile long totalInsertLatency;
    private volatile long updateCount;
    private volatile long totalUpdateLatency;
    private volatile long removeCount;
    private volatile long totalRemoveLatency;
    private volatile long memoryCost;

    private boolean hasQueries;

    public PartitionPerIndexStats() {
        this.creationTime = Clock.currentTimeMillis();
    }

    private void updateMemoryCost(long delta) {
        MEMORY_COST.lazySet(this, memoryCost + delta);
    }

    private void resetMemoryCost() {
        MEMORY_COST.lazySet(PartitionPerIndexStats.this, 0);
    }

    @Override
    public long makeTimestamp() {
        return Timer.nanos();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    @Override
    public void incrementQueryCount() {
        if (hasQueries) {
            QUERY_COUNT.lazySet(this, queryCount + 1);
        }
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
        return Double.longBitsToDouble(totalNormalizedHitCardinality);
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
    public long getMemoryCost() {
        return memoryCost;
    }

    @Override
    public void onInsert(long startNanos, IndexOperationStats operationStats, Index.OperationSource operationSource) {
        if (operationStats.getEntryCountDelta() == 0) {
            // no entries were inserted
            return;
        }

        if (operationSource == Index.OperationSource.USER) {
            TOTAL_INSERT_LATENCY.lazySet(this, totalInsertLatency + (Timer.nanosElapsed(startNanos)));
            INSERT_COUNT.lazySet(this, insertCount + 1);
        }
        ENTRY_COUNT.lazySet(this, entryCount + 1);
    }

    @Override
    public void onUpdate(long startNanos, IndexOperationStats operationStats, Index.OperationSource operationSource) {
        if (operationSource == Index.OperationSource.USER) {
            TOTAL_UPDATE_LATENCY.lazySet(this, totalUpdateLatency + (Timer.nanosElapsed(startNanos)));
            UPDATE_COUNT.lazySet(this, updateCount + 1);
        }
        ENTRY_COUNT.lazySet(this, entryCount + operationStats.getEntryCountDelta());
    }

    @Override
    public void onRemove(long startNanos, IndexOperationStats operationStats, Index.OperationSource operationSource) {
        if (operationStats.getEntryCountDelta() == 0) {
            // no entries were removed
            return;
        }

        if (operationSource == Index.OperationSource.USER) {
            TOTAL_REMOVE_LATENCY.lazySet(this, totalRemoveLatency + (Timer.nanosElapsed(startNanos)));
            REMOVE_COUNT.lazySet(this, removeCount + 1);
        }
        ENTRY_COUNT.lazySet(this, entryCount - 1);
    }

    @Override
    public void onClear() {
        ENTRY_COUNT.lazySet(this, 0);
    }

    @Override
    public void onIndexHit(long startNanos, long hitCardinality) {
        // To compute the average hit cardinality we need to track the total
        // cardinality of all of the hits and then divide it by the number of hits.
        // But since the number of the indexed entries may change with time, this
        // raw total cardinality value can't provide any useful information. So
        // instead we store the total normalized cardinality, which sums up the
        // individual hit cardinalities divided by the index size at the time of the
        // hit.

        hasQueries = true;

        long localEntryCount = entryCount;
        if (localEntryCount == 0) {
            // selecting nothing from nothing is not counted as a hit
            return;
        }

        TOTAL_HIT_LATENCY.lazySet(this, totalHitLatency + (Timer.nanosElapsed(startNanos)));
        HIT_COUNT.lazySet(this, hitCount + 1);

        // limit the cardinality for "safety"
        long adjustedHitCardinality = Math.min(hitCardinality, localEntryCount);

        // this will produce a value in [0.0, 1.0] range
        double normalizedHitCardinality = (double) adjustedHitCardinality / localEntryCount;

        double decodedTotalNormalizedHitCardinality = Double.longBitsToDouble(totalNormalizedHitCardinality);
        double newTotalNormalizedHitCardinality = decodedTotalNormalizedHitCardinality + normalizedHitCardinality;
        long newEncodedTotalNormalizedHitCardinality = Double.doubleToRawLongBits(newTotalNormalizedHitCardinality);
        TOTAL_NORMALIZED_HIT_CARDINALITY.lazySet(this, newEncodedTotalNormalizedHitCardinality);
    }

    @Override
    public void resetPerQueryStats() {
        hasQueries = false;
    }

    @Override
    public MemoryAllocator wrapMemoryAllocator(MemoryAllocator memoryAllocator) {
        return new MemoryAllocatorWithStats(memoryAllocator);
    }

    @Override
    public IndexOperationStats createOperationStats() {
        operationStats.reset();
        return operationStats;
    }

    private class MemoryAllocatorWithStats implements MemoryAllocator {

        private final MemoryAllocator delegate;

        MemoryAllocatorWithStats(MemoryAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public long allocate(long size) {
            long result = delegate.allocate(size);
            updateMemoryCost(size);
            return result;
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            long result = delegate.reallocate(address, currentSize, newSize);
            updateMemoryCost(newSize - currentSize);
            return result;
        }

        @Override
        public void free(long address, long size) {
            delegate.free(address, size);
            updateMemoryCost(-size);
        }

        @Override
        public void dispose() {
            delegate.dispose();
            resetMemoryCost();
        }
    }

}
