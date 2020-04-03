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

import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

/**
 * Executor which receives batches from a single inbox.
 */
public class ReceiveExec extends AbstractExec {
    /** Inbox. */
    private final Inbox inbox;

    /** Current batch. */
    private RowBatch curBatch;

    public ReceiveExec(int id, Inbox inbox) {
        super(id);

        this.inbox = inbox;
    }

    @Override
    protected void setup0(QueryFragmentContext ctx) {
        inbox.setup();
    }

    @Override
    public IterationResult advance0() {
        InboundBatch batch = inbox.poll();

        if (batch == null) {
            curBatch = null;

            return IterationResult.WAIT;
        }

        curBatch = batch.getBatch();

        return inbox.closed() ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
    }

    @Override
    public RowBatch currentBatch0() {
        return curBatch;
    }

    public Inbox getInbox() {
        return inbox;
    }
}
