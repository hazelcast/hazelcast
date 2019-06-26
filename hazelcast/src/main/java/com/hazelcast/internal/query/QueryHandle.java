package com.hazelcast.internal.query;

public interface QueryHandle {
    /**
     * @return Unique query ID.
     */
    QueryId getQueryId();
}
