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

    /** Current row. */
    private RowBatch curRow;

    public FilterExec(Exec upstream, Expression<Boolean> filter) {
        super(upstream);

        this.filter = filter;
    }

    @Override
    public IterationResult advance() {
        while (true) {
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            for (Row upstreamRow : state) {
                boolean matches = filter.eval(ctx, upstreamRow);

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
    public RowBatch currentBatch() {
        return curRow;
    }

    @Override
    protected void reset1() {
        curRow = null;
    }
}
