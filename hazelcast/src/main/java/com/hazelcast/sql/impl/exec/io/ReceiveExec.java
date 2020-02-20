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
import com.hazelcast.sql.impl.mailbox.SendBatch;
import com.hazelcast.sql.impl.mailbox.SingleInbox;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.List;

/**
 * Executor which receives batches from a single inbox.
 */
public class ReceiveExec extends AbstractExec {
    /** Inbox. */
    private final SingleInbox inbox;

    /** Current batch. */
    private RowBatch curBatch;

    /** Whether inbox is closed. */
    private boolean inboxDone;

    public ReceiveExec(int id, SingleInbox inbox) {
        super(id);

        this.inbox = inbox;
    }

    @Override
    public IterationResult advance0() {
        if (inboxDone) {
            throw new IllegalStateException("Should not be called.");
        }

        SendBatch batch = inbox.poll();

        if (batch == null) {
            return IterationResult.WAIT;
        }

        List<Row> rows = batch.getRows();

        curBatch = rows.isEmpty() ? null : new ListRowBatch(rows);

        if (inbox.closed()) {
            inboxDone = true;

            return IterationResult.FETCHED_DONE;
        } else {
            inbox.sendFlowControl();

            return IterationResult.FETCHED;
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return curBatch;
    }

    @Override
    public boolean canReset() {
        return false;
    }
}
