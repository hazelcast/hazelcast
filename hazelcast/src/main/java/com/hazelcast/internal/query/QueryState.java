package com.hazelcast.internal.query;

/**
 * Current state of the query.
 */
public class QueryState {

    public static final int RUNNING = 0;
    public static final int DONE = 1;
    public static final int CANCELLED = 2;

    private final int state;
    private final QueryCancelReason cancelReason;
    private final String errMsg;

    public QueryState(int state, QueryCancelReason cancelReason, String errMsg) {
        this.state = state;
        this.cancelReason = cancelReason;
        this.errMsg = errMsg;
    }


}
