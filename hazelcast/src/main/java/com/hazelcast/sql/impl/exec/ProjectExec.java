/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.List;

/**
 * Projection executor.
 */
public class ProjectExec extends AbstractUpstreamAwareExec {
    /** Projection expressions. */
    private final List<Expression> projections;

    /** Current row. */
    private RowBatch curRow;

    public ProjectExec(int id, Exec upstream, List<Expression> projections) {
        super(id, upstream);

        this.projections = projections;
    }

    @Override
    public IterationResult advance0() {
        while (true) {
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            Row upstreamRow = state.nextIfExists();

            if (upstreamRow != null) {
                curRow = projectRow(upstreamRow);

                return state.isDone() ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
            }

            if (state.isDone()) {
                curRow = EmptyRowBatch.INSTANCE;

                return IterationResult.FETCHED_DONE;
            }
        }
    }

    /**
     * Project upstream row.
     *
     * @param row Upstream row.
     * @return Projected row.
     */
    private HeapRow projectRow(Row row) {
        HeapRow projectedRow = new HeapRow(projections.size());

        int colIdx = 0;

        for (Expression projection : projections) {
            Object projectionRes = projection.eval(row);

            projectedRow.set(colIdx++, projectionRes);
        }

        return projectedRow;
    }

    @Override
    public RowBatch currentBatch0() {
        return curRow;
    }

    @Override
    protected void reset1() {
        curRow = null;
    }
}
