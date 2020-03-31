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

package com.hazelcast.sql.impl.exec.root;

import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Root executor which consumes results from the upstream stages and pass them to target consumer.
 */
public class RootExec extends AbstractUpstreamAwareExec {
    /** Consumer (user iterator, client listener, etc). */
    private final RootResultConsumer consumer;

    public RootExec(int id, Exec upstream, RootResultConsumer consumer) {
        super(id, upstream);

        this.consumer = consumer;
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        consumer.setup(ctx);
    }

    @Override
    public IterationResult advance0() {
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
                consumer.onDone();

                return IterationResult.FETCHED_DONE;
            }
        }
    }

    @Override
    public RowBatch currentBatch0() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public boolean canReset() {
        return false;
    }
}
