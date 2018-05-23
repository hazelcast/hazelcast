package com.hazelcast.monitor.impl;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * The implementation of internal indexes stats specialized for global indexes.
 * <p>
 * The main trait of the implementation is the concurrency support, which is
 * required for global indexes because they are shared among partitions.
 */
public class GlobalIndexesStats implements InternalIndexesStats {

    private static final AtomicLongFieldUpdater<GlobalIndexesStats> QUERY_COUNT = newUpdater(GlobalIndexesStats.class,
            "queryCount");
    private static final AtomicLongFieldUpdater<GlobalIndexesStats> INDEXED_QUERY_COUNT = newUpdater(GlobalIndexesStats.class,
            "indexedQueryCount");

    private volatile long queryCount = 0;
    private volatile long indexedQueryCount = 0;

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    @Override
    public void incrementQueryCount() {
        QUERY_COUNT.incrementAndGet(this);
    }

    @Override
    public long getIndexedQueryCount() {
        return indexedQueryCount;
    }

    @Override
    public void incrementIndexedQueryCount() {
        INDEXED_QUERY_COUNT.incrementAndGet(this);
    }

    @Override
    public InternalIndexStats createIndexStats(boolean ordered, boolean queryableEntriesAreCached) {
        return new GlobalIndexStats(ordered, queryableEntriesAreCached);
    }

}
