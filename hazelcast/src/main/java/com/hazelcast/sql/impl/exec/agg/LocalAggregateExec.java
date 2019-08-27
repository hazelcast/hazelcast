/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.exec.agg;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.data.DataWorker;

import java.util.List;
import java.util.Map;

/**
 * Executor that performs local-only aggregation. If the input is already sorted properly on the group key, then
 * only a single aggregated row is allocated at a time. Otherwise, the whole result set is consumed from the upstream.
 */
public class LocalAggregateExec extends AbstractUpstreamAwareExec {
    /** Number of keys in the group. */
    private final int groupKeySize;

    /** Accumulators. */
    private final List<AggregateExpression> accumulators;

    /** Whether group key columns are already sorted. */
    private final boolean sorted;

    /** Last upstream batch. */
    private RowBatch curBatch;

    /** Current position in the last upstream batch. */
    private int curBatchPos = -1;

    /** Maximum position in the last upstream batch. */
    private int curBatchRowCnt = -1;

    /** Current row. */
    private RowBatch curRow;

    /** Aggregated rows (for blocking mode). */
    private Map<AggregateKey, List<AggregateCollector>> map;

    /** Current single key (for non-blocking mode). */
    private AggregateKey singleKey;

    /** Current single values (for non-blocking mode). */
    private List<AggregateCollector> singleVals;

    public LocalAggregateExec(
        Exec upstream,
        int groupKeySize,
        List<AggregateExpression> accumulators,
        boolean sorted
    ) {
        super(upstream);

        this.groupKeySize = groupKeySize;
        this.accumulators = accumulators;
        this.sorted = sorted;
    }

    @Override
    protected void setup1(QueryContext ctx, DataWorker worker) {
        super.setup1(ctx, worker);
    }

    @Override
    public IterationResult advance() {
        // Fetch data in cycle because we do not know when to stop in advance.
        while (true) {
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

                        break;

                    case WAIT:
                        return IterationResult.WAIT;

                    default:
                        throw new IllegalStateException("Should not reach this.");
                }
            }

            IterationResult res = advanceCurrentBatch();

            if (res != null)
                return res;
        }
    }

    /**
     * Advance position in the current batch
     *
     * @return Iteration result is succeeded, {@code null} if all rows from the given batch were filtered.
     */
    private IterationResult advanceCurrentBatch() {
        // Handle empty batch.
        if (curBatch == null) {
            assert upstreamDone;

            curRow = EmptyRowBatch.INSTANCE;

            return IterationResult.FETCHED_DONE;
        }

        RowBatch curBatch0 = curBatch;
        int curBatchPos0 = curBatchPos;

        while (true) {
            Row candidateRow = curBatch0.getRow(curBatchPos0);

            AggregateKey key = createKeyFromRow(candidateRow);





            boolean matches = filter.eval(ctx, candidateRow);

            boolean last = curBatchPos0 + 1 == curBatchRowCnt;

            // Nullify state if this was the last entry in the upstream batch.
            if (last) {
                curBatch = null;
                curBatchPos = -1;
                curBatchRowCnt = -1;
            }

            if (matches) {
                curRow = candidateRow;

                if (last)
                    // Return DONE if the upstream is not going to provide more data.
                    return upstreamDone ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
                else {
                    // Advance batch position for the next call.
                    curBatchPos = curBatchPos0;

                    // This is not the last entry.
                    return IterationResult.FETCHED;
                }
            }
            else {
                if (last) {
                    if (upstreamDone) {
                        // Set empty batch for the downstream operator.
                        curRow = EmptyRowBatch.INSTANCE;

                        return IterationResult.FETCHED_DONE;
                    }
                    else
                        // Request next batch.
                        return null;
                }
            }

            curBatchPos0++;
        }
    }

    @Override
    public RowBatch currentBatch() {
        return curRow;
    }
}
