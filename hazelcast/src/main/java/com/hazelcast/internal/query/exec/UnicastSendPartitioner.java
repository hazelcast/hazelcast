package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.Row;

public class UnicastSendPartitioner implements SendPartitioner {
    /** Predefined outbox. */
    private final Outbox outbox;

    public UnicastSendPartitioner(Outbox outbox) {
        this.outbox = outbox;
    }

    @Override
    public Outbox map(QueryContext ctx, Row row) {
        return outbox;
    }
}
