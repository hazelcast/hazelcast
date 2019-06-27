package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.List;

public class ProjectExec extends AbstractUpstreamAwareExec {
    /** Projection expressions. */
    private final List<Expression> projections;

    /** Last upstream batch. */
    private RowBatch curBatch;

    /** Current position in the last upstream batch. */
    private int curBatchPos = -1;

    /** Maximum position in the last upstream batch. */
    private int curBatchRowCnt = -1;

    /** Current row. */
    private Row curRow;

    public ProjectExec(Exec upstream, List<Expression> projections) {
        super(upstream);

        this.projections = projections;
    }

    @Override
    public IterationResult advance() {
        if (curBatch == null) {
            if (upstreamDone)
                return IterationResult.FETCHED_DONE;

            switch (advanceUpstream()) {
                case FETCHED_DONE:
                case FETCHED:
                    RowBatch batch = upstreamCurrentBatch;
                    int batchRowCnt = batch.getRowCount();

                    if (batchRowCnt > 0) {
                        curBatch = batch;
                        curBatchPos = 0;
                        curBatchRowCnt = batchRowCnt;
                    }

                case WAIT:
                    return IterationResult.WAIT;

                default:
                    throw new IllegalStateException("Should not reach this.");
            }
        }

        return advanceCurrentBatch();
    }

    @Override
    public RowBatch currentBatch() {
        return curRow;
    }

    private IterationResult advanceCurrentBatch() {
        // Prepare the next row.
        Row upstreamRow = curBatch.getRow(curBatchPos);

        HeapRow curRow0 = new HeapRow(projections.size());

        int colIdx = 0;

        for (Expression projection : projections)
            curRow0.set(colIdx++, projection.eval(ctx, upstreamRow));

        curRow = curRow0;

        // Advance position.
        if (++curBatchPos == curBatchRowCnt) {
            curBatch = null;
            curBatchPos = -1;
            curBatchRowCnt = -1;

            if (upstreamDone)
                return IterationResult.FETCHED_DONE;
        }

        return IterationResult.FETCHED;
    }
}
