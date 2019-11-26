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

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.QueryResultConsumer;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.task.AdvanceRootQueryTask;

/**
 * Root executor which consumes results from the upstream stages and pass them to target consumer.
 */
public class RootExec extends AbstractUpstreamAwareExec {
    /** Consumer (user iterator, client listener, etc). */
    private QueryResultConsumer consumer;

    public RootExec(int id, Exec upstream) {
        super(id, upstream);
    }

    @Override
    protected void setup1(QueryContext ctx) {
        consumer = ctx.getRootConsumer();

        consumer.setup(this);
    }

    @Override
    public IterationResult advance() {
        while (true) {
            // Advance if needed.
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            // Try consuming as much rows as possible.
            if (!consumer.consume(state)) {
                return IterationResult.WAIT;
            }

            // Close the consumer if we reached the end.
            if (state.isDone()) {
                consumer.done();

                return IterationResult.FETCHED_DONE;
            }
        }
    }

    @Override
    public RowBatch currentBatch0() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    /**
     * Reschedule execution of this root node to fetch more data to the user.
     */
    public void reschedule() {
        AdvanceRootQueryTask task = new AdvanceRootQueryTask(this);

        ctx.getWorker().offer(task);
    }

    @Override
    public boolean canReset() {
        return false;
    }
}
