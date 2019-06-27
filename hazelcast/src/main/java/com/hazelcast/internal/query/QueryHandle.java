package com.hazelcast.internal.query;

/**
 * Handle to running query.
 */
public class QueryHandle {
    /** Query ID. */
    private final QueryId queryId;

    /** Consumer. */
    private final QueryResultConsumer consumer;

    public QueryHandle(QueryId queryId, QueryResultConsumer consumer) {
        this.queryId = queryId;
        this.consumer = consumer;
    }

    /**
     * @return Query ID.
     */
    public QueryId getQueryId() {
        return queryId;
    }

    /**
     * Close the handle.
     */
    public void close() {
        // TODO: Cancel/close support.
    }

    public QueryResultConsumer getConsumer() {
        return consumer;
    }
}
