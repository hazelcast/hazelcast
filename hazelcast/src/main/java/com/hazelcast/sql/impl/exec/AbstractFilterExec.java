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

import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract filter executor that removes rows from the output based on a condition.
 * <p>
 * Currently the executor batches rows, and reports progress only when the batch is full or when EOS has been reached.
 * This is done to minimize the operator evaluation overhead.
 * <p>
 * The compiled counterpart does not require batching.
 */
public abstract class AbstractFilterExec extends AbstractUpstreamAwareExec {

    static final int BATCH_SIZE = 1024;

    private List<Row> currentRows;
    private ListRowBatch currentBatch;

    protected AbstractFilterExec(int id, Exec upstream) {
        super(id, upstream);
    }

    @Override
    public IterationResult advance0() {
        if (currentRows == null) {
            currentRows = new ArrayList<>(BATCH_SIZE);
            currentBatch = null;
        }

        int count = currentRows.size();

        while (true) {
            // Wait if cannot get more rows.
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            // Consume results until the batch is full.
            for (Row upstreamRow : state) {
                boolean matches = eval(upstreamRow);

                if (matches) {
                    currentRows.add(upstreamRow);

                    if (++count == BATCH_SIZE) {
                        return prepareBatch(state.isDone() ? IterationResult.FETCHED_DONE : IterationResult.FETCHED);
                    }
                }
            }

            if (state.isDone()) {
                return prepareBatch(IterationResult.FETCHED_DONE);
            }
        }
    }

    private IterationResult prepareBatch(IterationResult result) {
        currentBatch = new ListRowBatch(currentRows);
        currentRows = null;

        return result;
    }

    @Override
    public RowBatch currentBatch0() {
        return currentBatch;
    }

    protected abstract boolean eval(Row row);
}
