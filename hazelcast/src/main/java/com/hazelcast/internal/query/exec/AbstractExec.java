package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;

public abstract class AbstractExec implements Exec {

    protected QueryContext ctx;

    @Override
    public void setup(QueryContext ctx) {
        this.ctx = ctx;

        setup0();
    }

    protected abstract void setup0();
}
