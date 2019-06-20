package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.io.HeapRowBatch;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.RowBatch;
import com.hazelcast.internal.query.io.SendBatch;

import java.util.List;

public class ReceiveExec extends AbstractExec {

    private final Inbox inbox;

    private RowBatch curBatch;
    private boolean last;

    public ReceiveExec(Inbox inbox) {
        this.inbox = inbox;
    }

    @Override
    public IterationResult advance() {
        if (last)
            throw new IllegalStateException("Should not be called.");

        SendBatch batch = inbox.poll();

        if (batch == null)
            return IterationResult.WAIT;

        List<Row> rows = batch.getRows();

        curBatch = new HeapRowBatch(rows);

        if (inbox.closed()) {
            last = true;

            return IterationResult.FETCHED_DONE;
        }
        else
            return IterationResult.FETCHED;
    }

    @Override
    public RowBatch currentBatch() {
        return curBatch;
    }
}
