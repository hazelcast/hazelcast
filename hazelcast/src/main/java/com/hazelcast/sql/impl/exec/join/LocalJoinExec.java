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

package com.hazelcast.sql.impl.exec.join;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.UpstreamState;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.JoinRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Executor for local join.
 */
public class LocalJoinExec extends AbstractUpstreamAwareExec {
    /** Right input. */
    private final UpstreamState rightState;

    /** Filter. */
    private final Expression<Boolean> filter;

    /** Current left row. */
    private Row leftRow;

    /** Current row. */
    private RowBatch curRow;

    public LocalJoinExec(Exec left, Exec right, Expression<Boolean> filter) {
        super(left);

        rightState = new UpstreamState(right);

        this.filter = filter;
    }

    @Override
    protected void setup1(QueryContext ctx) {
        rightState.setup(ctx);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public IterationResult advance() {
        while (true) {
            // Get the left row.
            if (leftRow == null) {
                while (true) {
                    if (!state.advance()) {
                        return IterationResult.WAIT;
                    }

                    leftRow = state.nextIfExists();

                    if (leftRow != null) {
                        rightState.reset();

                        break;
                    } else if (state.isDone()) {
                        curRow = EmptyRowBatch.INSTANCE;

                        return IterationResult.FETCHED_DONE;
                    }
                }
            }

            // Iterate over the right input.
            do {
                if (!rightState.advance()) {
                    return IterationResult.WAIT;
                }

                for (Row rightRow : rightState) {
                    JoinRow row = new JoinRow(leftRow, rightRow);

                    // TODO: This evaluation is very inefficient. Consider the query:
                    // TODO: "FROM A INNER JOIN B on A.a = B.b AND A.a = 10 AND B.b = 20"
                    // TODO: If the filters "A.a = 10" and "B.b = 20" are not pushed down in advance, then we
                    // TODO: will do a lot of unnecessary scans. So it is extremely important to make sure that
                    // TODO: the conjunctive component of the join condition is pushed down. E.g.:
                    // TODO: "filter A && filter B <- join <- scan A && scan B" should be converted to
                    // TODO: "join <- (filter A <- scan A) && (filter B <- scan B)".
                    // TODO: Calcite candidates: JoinPushTransitivePredicatesRule, JoinPushExpressionsRule,
                    // TODO: JoinPushThroughJoinRule

                    // Evaluate the condition.
                    if (filter.eval(ctx, row)) {
                        curRow = row;

                        if (state.isDone() && rightState.isDone()) {
                            leftRow = null;

                            return IterationResult.FETCHED_DONE;
                        }

                        return IterationResult.FETCHED;
                    }
                }

            } while (!rightState.isDone());

            // Nullify left row.
            leftRow = null;
        }
    }

    @Override
    public RowBatch currentBatch() {
        return curRow != null ? curRow : EmptyRowBatch.INSTANCE;
    }

    @Override
    public boolean canReset() {
        return super.canReset() && rightState.canReset();
    }

    @Override
    protected void reset1() {
        rightState.reset();

        curRow = null;
    }
}
