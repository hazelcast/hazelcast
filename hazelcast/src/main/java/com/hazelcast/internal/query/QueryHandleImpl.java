package com.hazelcast.internal.query;

public class QueryHandleImpl implements QueryHandle {
    /** Service */
    private final QueryService service;

    /** Query ID. */
    private final QueryId queryId;

    /** Consumer. */
    private final QueryResultConsumer consumer;

    public QueryHandleImpl(QueryService service, QueryId queryId, QueryResultConsumer consumer) {
        this.service = service;
        this.queryId = queryId;
        this.consumer = consumer;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    public QueryResultConsumer getConsumer() {
        return consumer;
    }
}
