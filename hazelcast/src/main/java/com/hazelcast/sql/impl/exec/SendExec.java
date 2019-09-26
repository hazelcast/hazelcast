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
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Executor which sends results to remote stripes.
 */
// TODO: Consider multiplexing.
public class SendExec extends AbstractUpstreamAwareExec {
    /** Expression to get the hash of the stripe. */
    private final Expression<Integer> partitionHasher;

    /** Outboxes. */
    private final Outbox[] outboxes;

    /** Row which pending send. */
    private Row pendingRow;

    public SendExec(Exec upstream, Expression<Integer> partitionHasher, Outbox[] outboxes) {
        super(upstream);

        this.partitionHasher = partitionHasher;
        this.outboxes = outboxes;
    }

    @Override
    public IterationResult advance() {
        if (pendingRow != null) {
            if (pushRow(pendingRow)) {
                pendingRow = null;
            } else {
                return IterationResult.WAIT;
            }
        }

        while (true) {
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            for (Row upstreamRow : state) {
                if (!pushRow(upstreamRow)) {
                    pendingRow = upstreamRow;

                    return IterationResult.WAIT;
                }
            }

            if (state.isDone()) {
                for (Outbox outbox : outboxes) {
                    outbox.flush();
                }

                return IterationResult.FETCHED_DONE;
            }
        }
    }

    /**
     * Push the current row to the outbox.
     *
     * @param row Row.
     * @return {@code True} if pushed successfully, {@code false}
     */
    private boolean pushRow(Row row) {
        int part = partitionHasher.eval(ctx, row);
        int idx =  part % outboxes.length;

        return outboxes[idx].onRow(row);
    }

    @Override
    public RowBatch currentBatch() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public boolean canReset() {
        // TODO: Sender is a top-level operator (similar to root), so currently no operator can initiate a reset on it.
        // TODO: However, in more complex operations, e.g. distributed joins, there is a chance that we will send
        // TODO: reset requests from receivers to their senders.
        return false;
    }
}
