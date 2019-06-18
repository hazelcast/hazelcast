package com.hazelcast.internal.query;

/**
 * Explains why the query was cancelled.
 */
public enum QueryCancelReason {
    /** User requested cancellation. */
    USER_REQUEST,

    /** Participating member has left the grid. */
    MEMBER_LEAVE,

    /** Partition has migrated during query execution. */
    PARTITION_MIGRATION,

    /** Timeout has been reached. */
    TIMEOUT,

    /** Unexpected error. */
    ERROR
}
