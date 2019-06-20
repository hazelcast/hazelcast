package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.EmptyRowBatch;
import com.hazelcast.internal.query.io.HeapRowBatch;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.RowBatch;
import com.hazelcast.internal.query.worker.data.DataWorker;

import java.util.function.Consumer;

/**
 * Scan over empty result-set.
 */
public class EmptyScanExec implements Exec {
    /** Singleton instance. */
    public static EmptyScanExec INSTANCE = new EmptyScanExec();

    private EmptyScanExec() {
        // No-op.
    }

    @Override
    public void setup(QueryContext ctx, DataWorker worker) {
        // No-op.
    }

    @Override
    public IterationResult advance() {
        return IterationResult.FETCHED_DONE;
    }

    @Override
    public RowBatch currentBatch() {
        return EmptyRowBatch.INSTANCE;
    }
}
