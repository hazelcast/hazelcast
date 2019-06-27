package com.hazelcast.internal.query;

public class QueryHandleImpl implements QueryHandle {
    /** Query ID. */
    private final QueryId queryId;

    /** Consumer. */
    private final QueryResultConsumer consumer;

    public QueryHandleImpl(QueryId queryId, QueryResultConsumer consumer) {
        this.queryId = queryId;
        this.consumer = consumer;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public void close() {
        // TODO: Cancel/close support.
    }

    public QueryResultConsumer getConsumer() {
        return consumer;
    }
}
