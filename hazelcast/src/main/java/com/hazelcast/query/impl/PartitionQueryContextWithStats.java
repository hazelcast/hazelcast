package com.hazelcast.query.impl;

import com.hazelcast.monitor.impl.InternalIndexStats;

import java.util.HashSet;

/**
 * Extends the basic query context to support the per-index stats tracking on
 * behalf of partitioned indexes.
 */
public class PartitionQueryContextWithStats extends QueryContext {

    private final HashSet<InternalIndexStats> trackedStats = new HashSet<InternalIndexStats>(8);

    /**
     * Constructs a new partition query context with stats for the given indexes.
     *
     * @param indexes the indexes to construct the new query context for.
     */
    public PartitionQueryContextWithStats(Indexes indexes) {
        super(indexes);
    }

    @Override
    void attachTo(Indexes indexes) {
        assert indexes == this.indexes;
        for (InternalIndexStats stats : trackedStats) {
            stats.resetPerQueryStats();
        }
        trackedStats.clear();
    }

    @Override
    public Index getIndex(String attributeName) {
        if (indexes == null) {
            return null;
        }

        InternalIndex index = indexes.getIndex(attributeName);
        if (index == null) {
            return null;
        }

        trackedStats.add(index.getIndexStats());

        return index;
    }

    @Override
    void applyPerQueryStats() {
        for (InternalIndexStats stats : trackedStats) {
            stats.incrementQueryCount();
        }
    }

}
