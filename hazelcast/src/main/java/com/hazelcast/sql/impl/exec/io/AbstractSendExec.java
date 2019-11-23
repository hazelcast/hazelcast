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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Abstract sender
 */
public abstract class AbstractSendExec extends AbstractUpstreamAwareExec {
    /** Registered outboxes. */
    protected final Outbox[] outboxes;

    /** Row which pending send. */
    private Row pendingRow;

    /** Done flag. */
    private boolean done;

    public AbstractSendExec(Exec upstream, Outbox[] outboxes) {
        super(upstream);

        this.outboxes = outboxes;
    }

    @Override
    public IterationResult advance() {
        if (done) {
            return IterationResult.FETCHED_DONE;
        }

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
                done = true;

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
    protected abstract boolean pushRow(Row row);

    @Override
    public RowBatch currentBatch0() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public boolean canReset() {
        return false;
    }
}
