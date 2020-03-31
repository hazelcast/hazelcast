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

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.row.hash.RowHashFunction;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Unicast sender.
 */
public class UnicastSendExec extends AbstractSendExec {
    /** Hash function. */
    private final RowHashFunction hashFunction;

    /** Contains index of partition outboxes. */
    private final int[] partitionOutboxIndexes;

    /** Pending states. */
    private final Map<Integer, State> pendingStates = new HashMap<>();

    /** Whether pending batch is the last one. */
    private boolean pendingLast;

    public UnicastSendExec(
        int id,
        Exec upstream,
        Outbox[] outboxes,
        RowHashFunction hashFunction,
        int[] partitionOutboxIndexes
    ) {
        super(id, upstream, outboxes);

        this.hashFunction = hashFunction;
        this.partitionOutboxIndexes = partitionOutboxIndexes;
    }

    @Override
    protected boolean pushPendingBatch() {
        if (pendingStates.isEmpty() && !pendingLast) {
            // Last batch must be flushed forcefully, this is why we proceed to push even of there are no pending states.
            return true;
        }

        return push0();
    }

    @Override
    protected boolean pushBatch(RowBatch batch, boolean last) {
        assert pendingStates.isEmpty();

        // Split batch rows by destinations.
        for (Row row : batch.getRows()) {
            int outboxIndex = resolveOutboxIndex(row);

            State state = pendingStates.get(outboxIndex);

            if (state == null) {
                state = new State();

                pendingStates.put(outboxIndex, state);
            }

            state.addRow(row);
        }

        // Send data to destinations.
        pendingLast = last;

        return push0();
    }

    private boolean push0() {
        // Send flush markers to outboxes without pending state.
        boolean res = true;

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

        // FLush pending states.
        Iterator<Map.Entry<Integer, State>> iterator = pendingStates.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, State> entry = iterator.next();

            int outboxIndex = entry.getKey();
            State state = entry.getValue();

            int rowCount = outboxes[outboxIndex].onRowBatch(state.rows, state.position, pendingLast);

            if (state.advancePosition(rowCount)) {
                iterator.remove();
            } else {
                res = false;
            }
        }

        return res;
    }

    private int resolveOutboxIndex(Row row) {
        if (outboxes.length == 1) {
            return 0;
        } else {
            int hash = hashFunction.getHash(row);
            int part = HashUtil.hashToIndex(hash, partitionOutboxIndexes.length);
            int outboxIndex = partitionOutboxIndexes[part];

            return outboxIndex;
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +  "{id=" + getId() + '}';
    }

    private static class State {
        private List<Row> rows;
        private int position;

        public void addRow(Row row) {
            if (rows == null) {
                // TODO: Avoid these constant reallocations!
                rows = new ArrayList<>();
            }

            rows.add(row);
        }

        public List<Row> getRows() {
            return rows;
        }

        public int getPosition() {
            return position;
        }

        public boolean advancePosition(int delta) {
            int position0 = position + delta;

            position = position0;

            return position0 == rows.size();
        }
    }

}
