package com.hazelcast.query.impl;

/**
 * Provides the query context support for global indexes with disabled stats.
 */
public class GlobalQueryContextProvider implements QueryContextProvider {

    private static final ThreadLocal<QueryContext> QUERY_CONTEXT = new ThreadLocal<QueryContext>() {
        @Override
        protected QueryContext initialValue() {
            return new QueryContext();
        }
    };

    @Override
    public QueryContext obtainContextFor(Indexes indexes) {
        QueryContext queryContext = QUERY_CONTEXT.get();
        queryContext.attachTo(indexes);
        return queryContext;
    }

}
