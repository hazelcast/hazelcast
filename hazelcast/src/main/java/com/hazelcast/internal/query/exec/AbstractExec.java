package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.worker.data.DataWorker;

public abstract class AbstractExec implements Exec {

    protected QueryContext ctx;

    @Override
    public void setup(QueryContext ctx, DataWorker worker) {
        this.ctx = ctx;

        setup0(ctx, worker);
    }

    protected void setup0(QueryContext ctx, DataWorker worker) {
        // No-op.
    }
}
