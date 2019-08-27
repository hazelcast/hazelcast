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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Filter executor.
 */
public class FilterExec extends AbstractUpstreamAwareExec {
    /** Filter. */
    private final Expression<Boolean> filter;

    /** Last upstream batch. */
    private RowBatch curBatch;

    /** Current position in the last upstream batch. */
    private int curBatchPos = -1;

    /** Current row. */
    private RowBatch curRow;

    public FilterExec(Exec upstream, Expression<Boolean> filter) {
        super(upstream);

        this.filter = filter;
    }

    @Override
    public IterationResult advance() {
        // Cycle is needed because some of the rows will be filtered, so we do not how many upstream rows to consume.
        while (true) {
            if (curBatch == null) {
                if (upstreamDone)
                    return IterationResult.FETCHED_DONE;

                switch (advanceUpstream()) {
                    case FETCHED_DONE:
                    case FETCHED:
                        RowBatch batch = upstreamCurrentBatch;

                        if (batch.getRowCount() > 0) {
                            curBatch = batch;
                            curBatchPos = 0;
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
     * @return Iteration result if succeeded, {@code null} if all rows from the given batch were filtered and more
     *    data from the upstream is required.
     */
    private IterationResult advanceCurrentBatch() {
        if (curBatch == null) {
            assert upstreamDone;

            curRow = EmptyRowBatch.INSTANCE;

            return IterationResult.FETCHED_DONE;
        }

        RowBatch curBatch0 = curBatch;
        int curBatchPos0 = curBatchPos;

        while (true) {
            Row candidateRow = curBatch0.getRow(curBatchPos0);

            boolean matches = filter.eval(ctx, candidateRow);
            boolean last = curBatchPos0 + 1 == curBatch0.getRowCount();

            // Nullify state if this was the last entry in the upstream batch.
            if (last) {
                curBatch = null;
                curBatchPos = -1;
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
                        // Request the next batch.
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
