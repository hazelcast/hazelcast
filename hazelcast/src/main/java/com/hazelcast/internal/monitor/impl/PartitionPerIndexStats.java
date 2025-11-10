/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.tpcengine.util.ReflectionUtil;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.query.impl.Index;
import com.hazelcast.internal.util.Clock;

import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * The implementation of internal index stats specialized for partitioned indexes.
 * <p>
 * The main trait of the implementation is the lack of concurrency support since
 * partitioned indexes and their stats are updated from a single thread only.
 */
public class PartitionPerIndexStats implements PerIndexStats {

    private static final VarHandle ENTRY_COUNT = ReflectionUtil.findVarHandle("entryCount", long.class);
    private static final VarHandle QUERY_COUNT = ReflectionUtil.findVarHandle("queryCount", long.class);
    private static final VarHandle HIT_COUNT = ReflectionUtil.findVarHandle("hitCount", long.class);
    private static final VarHandle TOTAL_HIT_LATENCY = ReflectionUtil.findVarHandle("totalHitLatency", long.class);
    private static final VarHandle TOTAL_NORMALIZED_HIT_CARDINALITY =
            ReflectionUtil.findVarHandle("totalNormalizedHitCardinality", long.class);
    private static final VarHandle INSERT_COUNT = ReflectionUtil.findVarHandle("insertCount", long.class);
    private static final VarHandle TOTAL_INSERT_LATENCY = ReflectionUtil.findVarHandle("totalInsertLatency", long.class);
    private static final VarHandle UPDATE_COUNT = ReflectionUtil.findVarHandle("updateCount", long.class);
    private static final VarHandle TOTAL_UPDATE_LATENCY = ReflectionUtil.findVarHandle("totalUpdateLatency", long.class);
    private static final VarHandle REMOVE_COUNT = ReflectionUtil.findVarHandle("removeCount", long.class);
    private static final VarHandle TOTAL_REMOVE_LATENCY = ReflectionUtil.findVarHandle("totalRemoveLatency", long.class);
    private static final VarHandle MEMORY_COST = ReflectionUtil.findVarHandle("memoryCost", long.class);
    private static final AtomicLongFieldUpdater<PartitionPerIndexStats> INDEX_NOT_READY_QUERY_COUNT =
            newUpdater(PartitionPerIndexStats.class, "indexNotReadyQueryCount");

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
    private volatile long partitionsIndexed;
    private volatile long indexNotReadyQueryCount;

    private boolean hasQueries;

    public PartitionPerIndexStats() {
        this.creationTime = Clock.currentTimeMillis();
    }

    @Override
    public void updateMemoryCost(long delta) {
        MEMORY_COST.setOpaque(this, memoryCost + delta);
    }

    @Override
    public void onDispose() {
        MEMORY_COST.setOpaque(PartitionPerIndexStats.this, 0);
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
            QUERY_COUNT.setOpaque(this, queryCount + 1);
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
            TOTAL_INSERT_LATENCY.setOpaque(this, totalInsertLatency + (Timer.nanosElapsed(startNanos)));
            INSERT_COUNT.setOpaque(this, insertCount + 1);
        }
        ENTRY_COUNT.setOpaque(this, entryCount + 1);
    }

    @Override
    public void onUpdate(long startNanos, IndexOperationStats operationStats, Index.OperationSource operationSource) {
        if (operationSource == Index.OperationSource.USER) {
            TOTAL_UPDATE_LATENCY.setOpaque(this, totalUpdateLatency + (Timer.nanosElapsed(startNanos)));
            UPDATE_COUNT.setOpaque(this, updateCount + 1);
        }
        ENTRY_COUNT.setOpaque(this, entryCount + operationStats.getEntryCountDelta());
    }

    @Override
    public void onRemove(long startNanos, IndexOperationStats operationStats, Index.OperationSource operationSource) {
        if (operationStats.getEntryCountDelta() == 0) {
            // no entries were removed
            return;
        }

        if (operationSource == Index.OperationSource.USER) {
            TOTAL_REMOVE_LATENCY.setOpaque(this, totalRemoveLatency + (Timer.nanosElapsed(startNanos)));
            REMOVE_COUNT.setOpaque(this, removeCount + 1);
        }
        ENTRY_COUNT.setOpaque(this, entryCount - 1);
    }

    @Override
    public void onClear() {
        ENTRY_COUNT.setOpaque(this, 0);
        partitionsIndexed = 0;
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

        TOTAL_HIT_LATENCY.setOpaque(this, totalHitLatency + (Timer.nanosElapsed(startNanos)));
        HIT_COUNT.setOpaque(this, hitCount + 1);

        // limit the cardinality for "safety"
        long adjustedHitCardinality = Math.min(hitCardinality, localEntryCount);

        // this will produce a value in [0.0, 1.0] range
        double normalizedHitCardinality = (double) adjustedHitCardinality / localEntryCount;

        double decodedTotalNormalizedHitCardinality = Double.longBitsToDouble(totalNormalizedHitCardinality);
        double newTotalNormalizedHitCardinality = decodedTotalNormalizedHitCardinality + normalizedHitCardinality;
        long newEncodedTotalNormalizedHitCardinality = Double.doubleToRawLongBits(newTotalNormalizedHitCardinality);
        TOTAL_NORMALIZED_HIT_CARDINALITY.setOpaque(this, newEncodedTotalNormalizedHitCardinality);
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

    @Override
    public long getPartitionsIndexed() {
        return partitionsIndexed;
    }

    @Override
    public long getPartitionUpdatesStarted() {
        return 0;
    }

    @Override
    public long getPartitionUpdatesFinished() {
        return 0;
    }

    @Override
    public void onPartitionChange(PartitionIndexChangeEvent changeEvent) {
        @SuppressWarnings("unused") long indexed = switch (changeEvent) {
            case INDEXED -> {
                partitionsIndexed = 1;
                yield partitionsIndexed;
            }
            case UNINDEXED -> {
                partitionsIndexed = 0;
                yield partitionsIndexed;
            }
            case CHANGE_STARTED, CHANGE_FINISHED -> partitionsIndexed;
        };
    }

    @Override
    public long getIndexNotReadyQueryCount() {
        return indexNotReadyQueryCount;
    }

    @Override
    public void incrementIndexNotReadyQueryCount() {
        INDEX_NOT_READY_QUERY_COUNT.incrementAndGet(this);
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
            onDispose();
        }
    }

}
