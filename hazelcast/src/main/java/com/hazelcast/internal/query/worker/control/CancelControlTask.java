package com.hazelcast.internal.query.worker.control;

import com.hazelcast.internal.query.QueryCancelReason;
import com.hazelcast.internal.query.QueryId;

public class CancelControlTask implements ControlTask {

    private final QueryId queryId;
    private final QueryCancelReason reason;
    private final String errorMessage;

    public CancelControlTask(QueryId queryId, QueryCancelReason reason, String errorMessage) {
        this.queryId = queryId;
        this.reason = reason;
        this.errorMessage = errorMessage;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    public QueryCancelReason getReason() {
        return reason;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
