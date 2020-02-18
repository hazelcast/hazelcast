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

/**
 * Abstract filter executor.
 */
public abstract class AbstractFilterExec extends AbstractUpstreamAwareExec {
    /** Current row. */
    private RowBatch curRow;

    protected AbstractFilterExec(int id, Exec upstream) {
        super(id, upstream);
    }

    @Override
    public IterationResult advance0() {
        while (true) {
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            for (Row upstreamRow : state) {
                boolean matches = eval(upstreamRow);

                if (matches) {
                    curRow = upstreamRow;

                    return state.isDone() ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
                }
            }

            if (state.isDone()) {
                curRow = EmptyRowBatch.INSTANCE;

                return IterationResult.FETCHED_DONE;
            }
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return curRow;
    }

    @Override
    protected void reset1() {
        curRow = null;
    }

    protected abstract boolean eval(Row row);
}
