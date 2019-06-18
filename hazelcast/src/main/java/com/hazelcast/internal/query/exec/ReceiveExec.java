package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.RowBatch;

import java.util.List;
import java.util.function.Consumer;

public class ReceiveExec extends AbstractExec {

    private final Inbox inbox;

    private List<Row> rows;
    private boolean last;

    public ReceiveExec(Inbox inbox) {
        this.inbox = inbox;
    }

    @Override
    protected void setup0() {
        // No-op.
    }

    @Override
    public IterationResult next() {
        if (rows == null) {
            if (last)
                throw new IllegalStateException("Should not be called.");

            RowBatch batch = inbox.poll();

            if (batch == null)
                return IterationResult.WAIT;

            rows = batch.getRows();

            if (inbox.closed()) {
                last = true;

                return IterationResult.FETCHED_DONE;
            }
            else
                return IterationResult.FETCHED;
        }
        else
            throw new IllegalStateException("Should not be called.");
    }

    @Override
    public void consume(Consumer<Row> consumer) {
        // TODO: Upper should not invoke it when there are no rows.
        if (!rows.isEmpty()) {
            consumer.accept(rows.get(0));

            // TODO: Dirty!
            rows.remove(0);
        }

        if (rows.isEmpty())
            rows = null;
    }

    @Override
    public int remainingRows() {
        return rows != null ? rows.size(): 0;
    }
}
