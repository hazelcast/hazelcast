package com.hazelcast.internal.query.exec;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.mailbox.Outbox;

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
