package com.hazelcast.query.impl;

/**
 * Provides the query context support for global indexes with enabled stats.
 */
public class GlobalQueryContextProviderWithStats implements QueryContextProvider {

    private static final ThreadLocal<GlobalQueryContextWithStats> QUERY_CONTEXT = new ThreadLocal<GlobalQueryContextWithStats>() {
        @Override
        protected GlobalQueryContextWithStats initialValue() {
            return new GlobalQueryContextWithStats();
        }
    };

    @Override
    public QueryContext obtainContextFor(Indexes indexes) {
        GlobalQueryContextWithStats queryContext = QUERY_CONTEXT.get();
        queryContext.attachTo(indexes);
        return queryContext;
    }

}
