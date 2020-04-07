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

import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

/**
 * Sender that communicates with a single outbox.
 */
public class SendExec extends AbstractSendExec {

    private final Outbox outbox;
    private int pendingPosition;

    public SendExec(int id, Exec upstream, Outbox outbox) {
        super(id, upstream);

        this.outbox = outbox;
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        outbox.setup();
    }

    @Override
    protected int getOutboxCount() {
        return 1;
    }

    @Override
    protected Outbox getOutbox(int outboxIndex) {
        return outbox;
    }

    @Override
    protected void setCurrentBatch(RowBatch batch) {
        // No-op.
    }

    @Override
    protected OutboxSendQualifier getOutboxQualifier(int outboxIndex) {
        return getOutboxQualifier();
    }

    @Override
    protected void addPendingPosition(int outboxIndex, int position) {
        pendingPosition = position;
    }

    @Override
    protected boolean pushPendingBatch(RowBatch pendingBatch, boolean pendingLast) {
        int newPosition = outbox.onRowBatch(pendingBatch, pendingLast, pendingPosition, getOutboxQualifier());

        if (newPosition == pendingBatch.getRowCount()) {
            pendingPosition = 0;

            return true;
        } else {
            pendingPosition = newPosition;

            return false;
        }
    }

    public Outbox getOutbox() {
        return outbox;
    }

    private AlwaysTrueOutboxSendQualifier getOutboxQualifier() {
        return AlwaysTrueOutboxSendQualifier.INSTANCE;
    }
}
