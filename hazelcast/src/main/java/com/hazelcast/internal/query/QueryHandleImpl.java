package com.hazelcast.internal.query;

import com.hazelcast.internal.query.exec.RootConsumer;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.List;
import java.util.function.Consumer;

public class QueryHandleImpl implements QueryHandle {
    /** Service */
    private final QueryService service;

    /** Query ID. */
    private final QueryId queryId;

    /** Consumer. */
    private final RootConsumer consumer;

    public QueryHandleImpl(QueryService service, QueryId queryId, RootConsumer consumer) {
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

    @Override
    public CompletedFuture<Void> consume(Consumer<List<Row>> consumer, int maxRows) {
        return null;
    }
}
