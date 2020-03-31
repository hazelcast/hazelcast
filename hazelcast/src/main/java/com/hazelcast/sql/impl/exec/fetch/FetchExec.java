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

package com.hazelcast.sql.impl.exec.fetch;

import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Executor to limit the number of rows.
 */
@SuppressWarnings("rawtypes")
public class FetchExec extends AbstractUpstreamAwareExec {
    private final Fetch fetch;
    private RowBatch currentBatch;

    public FetchExec(int id, Exec upstream, Expression fetch, Expression offset) {
        super(id, upstream);

        this.fetch = new Fetch(fetch, offset);
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        fetch.setup(ctx);
    }

    @Override
    protected IterationResult advance0() {
        currentBatch = null;

        while (true) {
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            RowBatch batch = state.consumeBatch();
            RowBatch newBatch = fetch.apply(batch);

            boolean done = fetch.isDone();

            if (done || newBatch.getRowCount() > 0) {
                currentBatch = newBatch;

                return done ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
            }
        }
    }

    @Override
    protected RowBatch currentBatch0() {
        return currentBatch;
    }
}
