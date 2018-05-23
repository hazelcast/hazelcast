package com.hazelcast.monitor.impl;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * The implementation of internal indexes stats specialized for partitioned
 * indexes.
 * <p>
 * The main trait of the implementation is the lack of concurrency support since
 * partitioned indexes and their stats are updated from a single thread only.
 */
public class PartitionIndexesStats implements InternalIndexesStats {

    private static final AtomicLongFieldUpdater<PartitionIndexesStats> QUERY_COUNT = newUpdater(PartitionIndexesStats.class,
            "queryCount");
    private static final AtomicLongFieldUpdater<PartitionIndexesStats> INDEXED_QUERY_COUNT = newUpdater(
            PartitionIndexesStats.class, "indexedQueryCount");

    private volatile long queryCount = 0;
    private volatile long indexedQueryCount = 0;

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    @Override
    public void incrementQueryCount() {
        QUERY_COUNT.lazySet(this, queryCount + 1);
    }

    @Override
    public long getIndexedQueryCount() {
        return indexedQueryCount;
    }

    @Override
    public void incrementIndexedQueryCount() {
        INDEXED_QUERY_COUNT.lazySet(this, indexedQueryCount + 1);
    }

    @Override
    public InternalIndexStats createIndexStats(boolean ordered, boolean queryableEntriesAreCached) {
        return new PartitionIndexStats();
    }

}
