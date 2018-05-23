package com.hazelcast.query.impl;

/**
 * Provides the query context support for partitioned indexes with enabled stats.
 */
public class PartitionQueryContextProviderWithStats implements QueryContextProvider {

    private final PartitionQueryContextWithStats queryContext;

    /**
     * Constructs a new partition query context provider with stats for the given
     * indexes.
     *
     * @param indexes the indexes to construct the new query context for.
     */
    public PartitionQueryContextProviderWithStats(Indexes indexes) {
        queryContext = new PartitionQueryContextWithStats(indexes);
    }

    @Override
    public QueryContext obtainContextFor(Indexes indexes) {
        queryContext.attachTo(indexes);
        return queryContext;
    }

}
