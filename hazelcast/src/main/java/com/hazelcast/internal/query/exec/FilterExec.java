package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.expression.Predicate;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Filter executor.
 */
public class FilterExec extends AbstractUpstreamAwareExec {
    /** Filter. */
    private final Predicate filter;

    /** Last upstream batch. */
    private RowBatch curBatch;

    /** Current position in the last upstream batch. */
    private int curBatchPos = -1;

    /** Maximum position in the last upstream batch. */
    private int curBatchRowCnt = -1;

    /** Current row. */
    private Row curRow;

    public FilterExec(Exec upstream, Predicate filter) {
        super(upstream);

        this.filter = filter;
    }

    @Override
    public IterationResult advance() {
        while (true) {
            // No batch -> need to fetch one.
            if (curBatch == null) {
                // Already fetched everything -> return.
                if (upstreamDone)
                    return IterationResult.FETCHED_DONE;

                switch (advanceUpstream()) {
                    case FETCHED_DONE:
                    case FETCHED:
                        RowBatch batch = upstreamCurrentBatch;
                        int batchRowCnt = batch.getRowCount();

                        if (batchRowCnt > 0) {
                            curBatch = batch;
                            curBatchPos = -1;
                            curBatchRowCnt = batchRowCnt;
                        }

                        break;

                    case WAIT:
                        return IterationResult.WAIT;

                    default:
                        throw new IllegalStateException("Should not reach this.");
                }
            }

            if (curBatch != null) {
                IterationResult res = advanceCurrentBatch();

                if (res != null)
                    return res;
            }
        }
    }

    /**
     * Advance position in the current batch
     *
     * @return Iteration result is succeeded, {@code null} if failed.
     */
    private IterationResult advanceCurrentBatch() {
        // Loop until the first matching row is found.
        assert curBatch != null;

        RowBatch curBatch0 = curBatch;
        int curBatchPos0 = curBatchPos;

        while (true) {
            curBatchPos0++;

            if (curBatchPos0 == curBatchRowCnt) {
                // Shifted behind -> nullify and return null.
                curBatch = null;
                curBatchPos = -1;
                curBatchRowCnt = -1;

                curRow = null;

                return upstreamDone ? IterationResult.FETCHED_DONE : null;
            }
            else {
                // Shifted successfully -> check filter match.
                Row candidateRow = curBatch0.getRow(curBatchPos0);

                if (filter.eval(ctx, candidateRow)) {
                    curBatchPos = curBatchPos0;
                    curRow = candidateRow;

                    if (curBatchPos0 + 1 == curBatchRowCnt && upstreamDone)
                        return IterationResult.FETCHED_DONE;
                    else
                        return IterationResult.FETCHED;
                }
            }
        }
    }

    @Override
    public RowBatch currentBatch() {
        return curRow;
    }
}
