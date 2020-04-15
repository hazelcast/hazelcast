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
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Project executor. Get rows from the incoming batch, projects them, and put into the output batch.
 */
@SuppressWarnings("rawtypes")
public class ProjectExec extends AbstractUpstreamAwareExec {

    private final List<Expression> projects;
    private RowBatch currentBatch;

    public ProjectExec(int id, Exec upstream, List<Expression> projects) {
        super(id, upstream);

        this.projects = projects;
    }

    @Override
    public IterationResult advance0() {
        while (true) {
            // Give up if no data is available.
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            // Skip empty input.
            RowBatch upstreamBatch = state.consumeBatch();

            if (upstreamBatch.getRowCount() == 0) {
                if (state.isDone()) {
                    currentBatch = EmptyRowBatch.INSTANCE;

                    return IterationResult.FETCHED_DONE;
                } else {
                    continue;
                }
            }

            // Perform projection.
            currentBatch = projectBatch(upstreamBatch);

            return state.isDone() ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return currentBatch;
    }

    public List<Expression> getProjects() {
        return projects;
    }

    private RowBatch projectBatch(RowBatch upstreamBatch) {
        List<Row> rows = new ArrayList<>(upstreamBatch.getRowCount());

        for (int i = 0; i < upstreamBatch.getRowCount(); i++) {
            Row upstreamRow = upstreamBatch.getRow(i);
            Row row = projectRow(upstreamRow);

            rows.add(row);
        }

        return new ListRowBatch(rows);
    }

    private Row projectRow(Row upstreamRow) {
        HeapRow row = new HeapRow(projects.size());

        int colIdx = 0;

        for (Expression<?> projection : projects) {
            Object projectionRes = projection.eval(upstreamRow, ctx);

            row.set(colIdx++, projectionRes);
        }

        return row;
    }
}
