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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class for senders that work with multiple outboxes.
 */
public abstract class AbstractMultiwaySendExec extends AbstractSendExec {

    protected final Outbox[] outboxes;
    private final Map<Integer, Position> pendingPositions = new HashMap<>();

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public AbstractMultiwaySendExec(int id, Exec upstream, Outbox[] outboxes) {
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
    protected int getOutboxCount() {
        return outboxes.length;
    }

    @Override
    protected Outbox getOutbox(int outboxIndex) {
        return outboxes[outboxIndex];
    }

    @Override
    protected void addPendingPosition(int outboxIndex, int position) {
        pendingPositions.put(outboxIndex, new Position(position));
    }

    @Override
    protected boolean pushPendingBatch(RowBatch pendingBatch, boolean pendingLast) {
        boolean res = true;

        Iterator<Map.Entry<Integer, Position>> iterator = pendingPositions.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, Position> entry = iterator.next();

            int outboxIndex = entry.getKey();
            Position position = entry.getValue();

            OutboxSendQualifier qualifier = getOutboxQualifier(outboxIndex);

            int newPosition = getOutbox(outboxIndex).onRowBatch(pendingBatch, pendingLast, position.get(), qualifier);

            if (newPosition == pendingBatch.getRowCount()) {
                iterator.remove();
            } else {
                position.set(newPosition);

                res = false;
            }
        }

        return res;
    }

    private static final class Position {

        private int value;

        private Position(int value) {
            this.value = value;
        }

        private int get() {
            return value;
        }

        private void set(int value) {
            this.value = value;
        }
    }
}
