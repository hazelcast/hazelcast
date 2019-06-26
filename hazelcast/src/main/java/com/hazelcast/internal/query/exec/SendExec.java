package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.RowBatch;
import com.hazelcast.internal.query.mailbox.Outbox;

public class SendExec extends AbstractUpstreamAwareExec {

    private final int edgeId;
    private final Expression<Integer> partitionHasher;
    private final Outbox[] outboxes;

    /** Last upstream batch. */
    private RowBatch curBatch;

    /** Current position in the last upstream batch. */
    private int curBatchPos = -1;

    /** Maximum position in the last upstream batch. */
    private int curBatchRowCnt = -1;

    public SendExec(Exec upstream, int edgeId, Expression<Integer> partitionHasher, Outbox[] outboxes) {
        super(upstream);

        this.edgeId = edgeId;
        this.partitionHasher = partitionHasher;
        this.outboxes = outboxes;
    }

    @Override
    public IterationResult advance() {
        while (true) {
            if (curBatch == null) {
                if (upstreamDone) {
                    for (Outbox outbox : outboxes)
                        outbox.flush();

                    return IterationResult.FETCHED_DONE;
                }

                switch (advanceUpstream()) {
                    case FETCHED_DONE:
                    case FETCHED:
                        RowBatch batch = upstreamCurrentBatch;
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
                        throw new IllegalStateException("Should not reach this.");
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
