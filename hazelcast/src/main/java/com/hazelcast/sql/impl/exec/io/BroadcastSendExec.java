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
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Broadcast sender.
 */
public class BroadcastSendExec extends AbstractSendExec {
    /** Pending rows. */
    private List<Row> pendingRows;

    /** Pending states. */
    private final Map<Integer, State> pendingStates = new HashMap<>();

    /** Whether pending batch is the last one. */
    private boolean pendingLast;

    public BroadcastSendExec(int id, Exec upstream, Outbox[] outboxes) {
        super(id, upstream, outboxes);
    }

    @Override
    protected boolean pushPendingBatch() {
        // If there are no pending rows, then all data has been flushed.
        if (pendingRows == null) {
            return true;
        }

        boolean res = true;

        // If we are flushing the last batch, then make sure that batches without pending state are notified.
        if (pendingLast) {
            for (int i = 0; i < outboxes.length; i++) {
                Outbox outbox = outboxes[i];

                if (pendingStates.containsKey(i)) {
                    continue;
                }

                if (!outbox.flushLastIfNeeded()) {
                    res = false;
                }
            }
        }

        // Notify batches with pending state.
        Iterator<Map.Entry<Integer, State>> iterator = pendingStates.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, State> entry = iterator.next();

            int outboxIndex = entry.getKey();
            State state = entry.getValue();

            int rowCount = outboxes[outboxIndex].onRowBatch(pendingRows, state.position, pendingLast);

            if (state.advancePosition(rowCount) == pendingRows.size()) {
                iterator.remove();
            } else {
                res = false;
            }
        }

        if (res) {
            pendingRows = null;

            return true;
        } else {
            return false;
        }
    }

    @Override
    protected boolean pushBatch(RowBatch batch, boolean last) {
        assert pendingRows == null;
        assert pendingStates.isEmpty();

        if (last) {
            pendingLast = true;
        }

        List<Row> rows = batch.getRows();

        boolean finished = true;

        for (int outboxIndex = 0; outboxIndex < outboxes.length; outboxIndex++) {
            int rowCount = outboxes[outboxIndex].onRowBatch(rows, 0, last);

            if (rowCount < rows.size()) {
                if (pendingRows == null) {
                    pendingRows = rows;
                    pendingStates.put(outboxIndex, new State(rowCount));
                }

                finished = false;
            }
        }

        return finished;
    }

    private static final class State {
        private int position;

        private State(int position) {
            this.position = position;
        }

        public int getPosition() {
            return position;
        }

        public int advancePosition(int delta) {
            int position0 = position + delta;

            position = position0;

            return position0;
        }
    }
}
