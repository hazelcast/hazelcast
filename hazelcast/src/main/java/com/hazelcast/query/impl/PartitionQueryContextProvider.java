package com.hazelcast.query.impl;

/**
 * Provides the query context support for partitioned indexes with disabled stats.
 */
public class PartitionQueryContextProvider implements QueryContextProvider {

    private final QueryContext queryContext;

    /**
     * Constructs a new partition query context provider for the given indexes.
     *
     * @param indexes the indexes to construct the new query context for.
     */
    public PartitionQueryContextProvider(Indexes indexes) {
        queryContext = new QueryContext(indexes);
    }

    @Override
    public QueryContext obtainContextFor(Indexes indexes) {
        assert indexes == queryContext.indexes;
        return queryContext;
    }

}
