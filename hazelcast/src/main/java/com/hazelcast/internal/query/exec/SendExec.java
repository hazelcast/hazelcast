package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.io.Row;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SendExec extends AbstractExec {
    private final Exec child;

    private final int edgeId;
    private final Expression<Integer> partitionHasher;
    private final Outbox[] outboxes;

    private boolean done;

    public SendExec(Exec child, int edgeId, Expression<Integer> partitionHasher, Outbox[] outboxes) {
        this.child = child;
        this.edgeId = edgeId;
        this.partitionHasher = partitionHasher;
        this.outboxes = outboxes;
    }

    @Override
    protected void setup0() {
        child.setup(ctx);
    }

    private boolean drainRow() {
        // TODO: We need this construct due to be iterator inteface. Need to think how to fix it.
        AtomicBoolean res0 = new AtomicBoolean();

        child.consume((row) -> {
            int part = partitionHasher.eval(ctx, row);

            int idx =  part % outboxes.length;

            // TODO: This way we may got stuck with a single slow remote node. Need to use batches!
            boolean res = outboxes[idx].onRow(row);

            res0.set(res);
        });

        return res0.get();
    }

    @Override
    public IterationResult next() {
        // Drain existing rows if any.
        while (child.remainingRows() > 0) {
            if (!drainRow())
                // Cannot push more rows to remote node. Wait.
                return IterationResult.WAIT;
        }

        if (done) {
            for (Outbox outbox : outboxes)
                outbox.close();

            return IterationResult.FETCHED_DONE;
        }

        assert child.remainingRows() == 0;

        while (true) {
            IterationResult res = child.next();

            switch (res) {
                case FETCHED_DONE:
                    done = true;

                    // Intentional fall-through.

                case FETCHED:
                    while (child.remainingRows() > 0) {
                        if (!drainRow())
                            // Cannot push more rows to remote node. Wait.
                            return IterationResult.WAIT;
                    }

                    // All rows are processed.
                    if (done) {
                        for (Outbox outbox : outboxes)
                            outbox.close();

                        return IterationResult.FETCHED_DONE;
                    }
                    else
                        continue;

                case ERROR:
                    // TODO
                    return IterationResult.ERROR;

                case WAIT:
                    return IterationResult.WAIT;
            }

            // TODO: Remove with proper switching.
            throw new UnsupportedOperationException("Should not reach this place");
        }
    }

    @Override
    public void consume(Consumer<Row> consumer) {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public int remainingRows() {
        return 0;
    }
}
