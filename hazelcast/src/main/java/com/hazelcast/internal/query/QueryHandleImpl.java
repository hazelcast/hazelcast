package com.hazelcast.internal.query;

public class QueryHandleImpl implements QueryHandle {
    /** Service */
    private final QueryService service;

    /** Query ID. */
    private final QueryId queryId;

    /** Consumer. */
    private final QueryRootConsumer consumer;

    public QueryHandleImpl(QueryService service, QueryId queryId, QueryRootConsumer consumer) {
        this.service = service;
        this.queryId = queryId;
        this.consumer = consumer;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public boolean cancel(QueryCancelReason reason, String errMsg) {
//        if (state.compareAndSet(QueryState.RUNNING, QueryState.CANCELLED)) {
//            // TODO
//        }

        return false;
    }

    public QueryRootConsumer getConsumer() {
        return consumer;
    }
}
