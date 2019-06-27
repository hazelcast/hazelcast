package com.hazelcast.internal.query.exec;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.internal.query.worker.data.DataWorker;

/**
 * Basic execution stage.
 */
public interface Exec {
    /**
     * One-time setup of the executor.
     *
     * @param ctx Context.
     * @param worker Worker responsible for execution of this instance.
     */
    void setup(QueryContext ctx, DataWorker worker);

    /**
     * Try advancing executor. Content of the current batch will be changed as a result of this call.
     *
     * @return Result of iteration.
     */
    IterationResult advance();

    /**
     * @return Current batch available in response to the previous {@link #advance()} call. Should never be null.
     */
    RowBatch currentBatch();
}
