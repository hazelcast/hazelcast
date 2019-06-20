package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.RowBatch;
import com.hazelcast.internal.query.worker.data.DataWorker;

public class SendExec extends AbstractExec {
    private final Exec upstream;

    private final int edgeId;
    private final Expression<Integer> partitionHasher;
    private final Outbox[] outboxes;

    private boolean upstreamDone;

    /** Last upstream batch. */
    private RowBatch curBatch;

    /** Current position in the last upstream batch. */
    private int curBatchPos = -1;

    /** Maximum position in the last upstream batch. */
    private int curBatchRowCnt = -1;

    public SendExec(Exec upstream, int edgeId, Expression<Integer> partitionHasher, Outbox[] outboxes) {
        this.upstream = upstream;
        this.edgeId = edgeId;
        this.partitionHasher = partitionHasher;
        this.outboxes = outboxes;
    }

    @Override
    protected void setup0(QueryContext ctx, DataWorker worker) {
        upstream.setup(ctx, worker);
    }

    @Override
    public IterationResult advance() {
        while (true) {
            if (curBatch == null) {
                if (upstreamDone) {
                    for (Outbox outbox : outboxes)
                        outbox.close();

                    return IterationResult.FETCHED_DONE;
                }

                switch (upstream.advance()) {
                    case FETCHED_DONE:
                        upstreamDone = true;

                        // Fall-through.

                    case FETCHED:
                        RowBatch batch = upstream.currentBatch();
                        int batchRowCnt = batch.getRowCount();

                        if (batchRowCnt == 0)
                            continue;

                        curBatch = batch;
                        curBatchPos = 0;
                        curBatchRowCnt = batchRowCnt;

                        break;

                    case WAIT:
                        return IterationResult.WAIT;

                    default:
                        // TODO: Error handling.
                        throw new UnsupportedOperationException("Implement me");
                }
            }

            if (!pushRows())
                return IterationResult.WAIT;
        }
    }

    @Override
    public RowBatch currentBatch() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    private boolean pushRows() {
        int curBatchPos0 = curBatchPos;

        for (; curBatchPos0 < curBatchRowCnt; curBatchPos0++) {
            Row row = curBatch.getRow(curBatchPos0);

            int part = partitionHasher.eval(ctx, row);
            int idx =  part % outboxes.length;

            // TODO: Bad: one slow output will not allow other proceed. How to fix that?
            if (!outboxes[idx].onRow(row)) {
                curBatchPos = curBatchPos0;

                return false;
            }
        }

        curBatch = null;
        curBatchPos = -1;
        curBatchRowCnt = -1;

        return true;
    }
}
