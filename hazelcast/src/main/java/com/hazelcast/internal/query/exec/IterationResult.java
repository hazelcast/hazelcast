package com.hazelcast.internal.query.exec;

/**
 * Result of {@link Exec#advance()} call.
 */
public enum IterationResult {
    /** One or more rows are available, and more rows may follow on subsequent {@link Exec#advance()} calls. */
    FETCHED,

    /** Zero or more rows available and no more rows will be supplied. */
    FETCHED_DONE,

    /** More rows may arrive in future, but none are available at the moment. Executor should give up the thread. */
    WAIT
}
