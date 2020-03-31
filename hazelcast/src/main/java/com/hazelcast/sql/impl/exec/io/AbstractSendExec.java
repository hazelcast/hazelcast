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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Abstract sender
 */
public abstract class AbstractSendExec extends AbstractUpstreamAwareExec {
    /** Registered outboxes. */
    protected final Outbox[] outboxes;

    /** Done flag. */
    private boolean done;

    public AbstractSendExec(int id, Exec upstream, Outbox[] outboxes) {
        super(id, upstream);

        this.outboxes = outboxes;
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        for (Outbox outbox : outboxes) {
            outbox.setup();
        }
    }

    @Override
    public IterationResult advance0() {
        if (done) {
            return IterationResult.FETCHED_DONE;
        }

        // Try finalizing the previous batch.
        if (!pushPendingBatch()) {
            return IterationResult.WAIT;
        }

        // Stop if state is exhausted.
        if (state.isDone()) {
            done = true;

            return IterationResult.FETCHED_DONE;
        }

        while (true) {
            // Try shifting the state.
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            // Push batch to outboxes.
            RowBatch batch = state.consumeBatch();

            boolean last = state.isDone();

            boolean pushed = pushBatch(batch, last);

            if (pushed) {
                if (last) {
                    // Pushed the very last batch, done.
                    done = true;

                    return IterationResult.FETCHED_DONE;
                } else {
                    // More batches to follow, repeat the loop.
                    continue;
                }
            }

            // Failed to push batch to all outboxes due to backpressure.
            return IterationResult.WAIT;
        }
    }

    /**
     * Push pending batches if needed.
     *
     * @return {@code True} if there are no more pending batches.
     */
    protected abstract boolean pushPendingBatch();

    /**
     * Push the current row batch to the outbox.
     *
     * @param batch Row batch.
     * @param last Whether this is the last batch.
     * @return {@code True} if the batch was accepted by all inboxes.
     */
    protected abstract boolean pushBatch(RowBatch batch, boolean last);

    @Override
    public RowBatch currentBatch0() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public boolean canReset() {
        return false;
    }
}
