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
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import java.util.ArrayList;

/**
 * Root executor which consumes results from the upstream stages and pass them to target consumer.
 */
public class RootExec extends AbstractUpstreamAwareExec {

    private final RootResultConsumer consumer;
    private final int batchSize;

    /** Current rows that are prepared for the consumer. */
    private ArrayList<Row> batch;

    public RootExec(int id, Exec upstream, RootResultConsumer consumer, int batchSize) {
        super(id, upstream);

        this.consumer = consumer;
        this.batchSize = batchSize;

        batch = new ArrayList<>(batchSize);
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        consumer.setup(ctx);
    }

    @Override
    public IterationResult advance0() {
        while (true) {
            // Consume the previous batch if needed.
            int remaining = batchSize - batch.size();

            boolean upstreamDone = state.isDone();

            if (remaining == 0 || upstreamDone) {
                if (consumer.consume(batch, upstreamDone)) {
                    // Batch has been consumed successfully.
                    if (upstreamDone) {
                        // Pushed the very last batch, done.
                        return IterationResult.FETCHED_DONE;
                    } else {
                        // Pushed the batch, but there are more to come, allocate the new batch and continue.
                        batch = new ArrayList<>(batchSize);

                        remaining = batchSize;
                    }
                } else {
                    // Cannot push to the consumer => WAIT.
                    return IterationResult.WAIT;
                }
            }

            assert remaining != 0;

            // Get more rows from the upstream.
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            for (Row row : state) {
                batch.add(row);

                if (--remaining == 0) {
                    break;
                }
            }
        }
    }

    @Override
    public RowBatch currentBatch0() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    public RootResultConsumer getConsumer() {
        return consumer;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
