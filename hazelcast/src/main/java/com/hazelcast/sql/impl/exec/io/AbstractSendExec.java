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
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Abstract sender
 */
public abstract class AbstractSendExec extends AbstractUpstreamAwareExec {
    /** Batch that is pending sending. */
    private RowBatch pendingBatch;

    /** Whether pending batch is the last one. */
    private boolean pendingLast;

    public AbstractSendExec(int id, Exec upstream) {
        super(id, upstream);
    }

    @Override
    public IterationResult advance0() {
        // Try finalizing the previous batch.
        if (!pushPendingBatch()) {
            return IterationResult.WAIT;
        }

        // Stop if state is exhausted.
        if (state.isDone()) {
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
     * @return {@code true} if there are no more pending batches.
     */
    private boolean pushPendingBatch() {
        // If there are no pending rows, then all data has been flushed.
        if (pendingBatch == null) {
            return true;
        }

        boolean res = pushPendingBatch(pendingBatch, pendingLast);

        if (res) {
            pendingBatch = null;

            return true;
        } else {
            return false;
        }
    }

    /**
     * Push the current row batch to the outbox.
     *
     * @param batch Row batch.
     * @param last Whether this is the last batch.
     * @return {@code true} if the batch was accepted by all inboxes.
     */
    private boolean pushBatch(RowBatch batch, boolean last) {
        // Pending state must be cleared at this point.
        assert pendingBatch == null;

        // Let the sender know that the new batch is being processed.
        setCurrentBatch(batch);

        // Try pushing the batch to as many outboxes as possible, logging the pending state along the way.
        boolean res = true;

        for (int outboxIndex = 0; outboxIndex < getOutboxCount(); outboxIndex++) {
            OutboxSendQualifier qualifier = getOutboxQualifier(outboxIndex);

            int position = getOutbox(outboxIndex).onRowBatch(batch, last, 0, qualifier);

            if (position < batch.getRowCount()) {
                if (pendingBatch == null) {
                    pendingBatch = batch;
                    pendingLast = last;
                }

                addPendingPosition(outboxIndex, position);

                res = false;
            }
        }

        return res;
    }

    @Override
    public RowBatch currentBatch0() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    protected abstract int getOutboxCount();

    protected abstract Outbox getOutbox(int outboxIndex);

    protected abstract void setCurrentBatch(RowBatch batch);

    protected abstract OutboxSendQualifier getOutboxQualifier(int outboxIndex);

    protected abstract void addPendingPosition(int outboxIndex, int position);

    protected abstract  boolean pushPendingBatch(RowBatch pendingBatch, boolean pendingLast);
}
