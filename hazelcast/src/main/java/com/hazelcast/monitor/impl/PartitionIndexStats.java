package com.hazelcast.monitor.impl;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * The implementation of internal index stats specialized for partitioned indexes.
 * <p>
 * The main trait of the implementation is the lack of concurrency support since
 * partitioned indexes and their stats are updated from a single thread only.
 */
public class PartitionIndexStats implements InternalIndexStats {

    private static final AtomicLongFieldUpdater<PartitionIndexStats> ENTRY_COUNT = newUpdater(PartitionIndexStats.class,
            "entryCount");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> QUERY_COUNT = newUpdater(PartitionIndexStats.class,
            "queryCount");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> HIT_COUNT = newUpdater(PartitionIndexStats.class,
            "hitCount");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> TOTAL_HIT_LATENCY = newUpdater(PartitionIndexStats.class,
            "totalHitLatency");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> TOTAL_NORMALIZED_HIT_CARDINALITY = newUpdater(
            PartitionIndexStats.class, "totalNormalizedHitCardinality");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> INSERT_COUNT = newUpdater(PartitionIndexStats.class,
            "insertCount");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> TOTAL_INSERT_LATENCY = newUpdater(PartitionIndexStats.class,
            "totalInsertLatency");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> UPDATE_COUNT = newUpdater(PartitionIndexStats.class,
            "updateCount");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> TOTAL_UPDATE_LATENCY = newUpdater(PartitionIndexStats.class,
            "totalUpdateLatency");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> REMOVE_COUNT = newUpdater(PartitionIndexStats.class,
            "removeCount");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> TOTAL_REMOVE_LATENCY = newUpdater(PartitionIndexStats.class,
            "totalRemoveLatency");
    private static final AtomicLongFieldUpdater<PartitionIndexStats> MEMORY_COST = newUpdater(PartitionIndexStats.class,
            "memoryCost");

    private final long creationTime;

    private volatile long entryCount = 0;
    private volatile long queryCount = 0;
    private volatile long hitCount = 0;
    private volatile long totalHitLatency = 0;
    private volatile long totalNormalizedHitCardinality = Double.doubleToRawLongBits(0.0);
    private volatile long insertCount = 0;
    private volatile long totalInsertLatency = 0;
    private volatile long updateCount = 0;
    private volatile long totalUpdateLatency = 0;
    private volatile long removeCount = 0;
    private volatile long totalRemoveLatency = 0;
    private volatile long memoryCost = 0;

    private boolean hasQueries;

    public PartitionIndexStats() {
        this.creationTime = Clock.currentTimeMillis();
    }

    private void updateMemoryCost(long delta) {
        MEMORY_COST.lazySet(this, memoryCost + delta);
    }

    private void resetMemoryCost() {
        MEMORY_COST.lazySet(PartitionIndexStats.this, 0);
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
    public long getOnHeapMemoryCost() {
        return 0;
    }

    @Override
    public long getOffHeapMemoryCost() {
        return memoryCost;
    }

    @Override
    public void onEntryInserted(long timestamp, Object value) {
        TOTAL_INSERT_LATENCY.lazySet(this, totalInsertLatency + (System.nanoTime() - timestamp));
        INSERT_COUNT.lazySet(this, insertCount + 1);
        ENTRY_COUNT.lazySet(this, entryCount + 1);
    }

    @Override
    public void onEntryUpdated(long timestamp, Object oldValue, Object newValue) {
        TOTAL_UPDATE_LATENCY.lazySet(this, totalUpdateLatency + (System.nanoTime() - timestamp));
        UPDATE_COUNT.lazySet(this, updateCount + 1);
    }

    @Override
    public void onEntryRemoved(long timestamp, Object value) {
        TOTAL_REMOVE_LATENCY.lazySet(this, totalRemoveLatency + (System.nanoTime() - timestamp));
        REMOVE_COUNT.lazySet(this, removeCount + 1);
        ENTRY_COUNT.lazySet(this, entryCount - 1);
    }

    @Override
    public void onEntriesCleared() {
        ENTRY_COUNT.lazySet(this, 0);
    }

    @Override
    public void onIndexHit(long timestamp, long hitCardinality) {
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

        TOTAL_HIT_LATENCY.lazySet(this, totalHitLatency + (System.nanoTime() - timestamp));
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

    private class MemoryAllocatorWithStats implements MemoryAllocator {

        private final MemoryAllocator delegate;

        public MemoryAllocatorWithStats(MemoryAllocator delegate) {
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
