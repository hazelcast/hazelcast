package com.hazelcast.internal.query.exec;

public enum IterationResult {
    /** More rows available. */
    FETCHED,

    /** Zero or more rows available, but definitely no more data. */
    FETCHED_DONE,

    // TODO: New schema

    /** Wait for data arrival. */
    WAIT,

    /** Error during query execution. Consult to the stage for more info. */
    ERROR
}
