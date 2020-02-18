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

import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Materializer for upstream input.
 */
public class MaterializedInputExec extends AbstractUpstreamAwareExec {
    /** Cached rows. */
    private List<Row> rows = new ArrayList<>();

    /** Current row. */
    private RowBatch curRow;

    /** Current position. */
    private int curPos;

    public MaterializedInputExec(int id, Exec upstream) {
        super(id, upstream);
    }

    @Override
    public IterationResult advance0() {
        while (curPos == rows.size()) {
            // Need to fetch more rows from upstream.
            if (state.isDone()) {
                // No more results from the upstream, we are done.
                curRow = EmptyRowBatch.INSTANCE;

                return IterationResult.FETCHED_DONE;
            } else {
                // Try advancing upstream.
                if (!state.advance()) {
                    // Cannot advance, let's wait.
                    return IterationResult.WAIT;
                } else {
                    // Advanced, consume rows.
                    for (Row row : state) {
                        rows.add(row);
                    }
                }
            }
        }

        assert curPos < rows.size();

        curRow = rows.get(curPos);

        curPos++;

        return curPos == rows.size() && state.isDone() ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
    }

    @Override
    public RowBatch currentBatch0() {
        return curRow;
    }

    @Override
    public boolean canReset() {
        return true;
    }

    @Override
    protected void reset0() {
        curRow = null;
        curPos = 0;
    }
}
