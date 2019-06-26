package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.worker.data.DataWorker;

/**
 * Abstract executor.
 */
public abstract class AbstractExec implements Exec {
    /** Global query context. */
    protected QueryContext ctx;

    @Override
    public final void setup(QueryContext ctx, DataWorker worker) {
        this.ctx = ctx;

        setup0(ctx, worker);
    }

    protected void setup0(QueryContext ctx, DataWorker worker) {
        // No-op.
    }
}
