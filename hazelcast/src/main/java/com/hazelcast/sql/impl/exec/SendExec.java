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
public class SendExec extends AbstractUpstreamAwareExec {
    /** Expression to get the hash of the stripe. */
    // TODO TODO: Something is wrong with this abstraction as it is tightly bound to outbox resolution. Rework.
    private final Expression<Integer> partitionHasher;

    /** Outboxes. */
    private final Outbox[] outboxes;

    /** Last upstream batch. */
    private RowBatch curBatch;

    /** Current position in the last upstream batch. */
    private int curBatchPos = -1;

    /** Maximum position in the last upstream batch. */
    private int curBatchRowCnt = -1;

    public SendExec(Exec upstream, Expression<Integer> partitionHasher, Outbox[] outboxes) {
        super(upstream);

        this.partitionHasher = partitionHasher;
        this.outboxes = outboxes;
    }

    @Override
    public IterationResult advance() {
        while (true) {
            if (curBatch == null) {
                if (upstreamDone) {
                    for (Outbox outbox : outboxes)
                        outbox.flush();

                    return IterationResult.FETCHED_DONE;
                }

                switch (advanceUpstream()) {
                    case FETCHED_DONE:
                    case FETCHED:
                        RowBatch batch = upstreamCurrentBatch;
                        int batchRowCnt = batch.getRowCount();

                        if (batchRowCnt == 0)
                            continue;

                        curBatch = batch;
                        curBatchPos = 0;
                        curBatchRowCnt = batchRowCnt;

                        break;

                    case WAIT:
                        return IterationResult.WAIT;

                    default:
                        throw new IllegalStateException("Should not reach this.");
                }
            }

            if (!pushRows())
                return IterationResult.WAIT;
        }
    }

    @Override
    public RowBatch currentBatch() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    private boolean pushRows() {
        int curBatchPos0 = curBatchPos;

        for (; curBatchPos0 < curBatchRowCnt; curBatchPos0++) {
            Row row = curBatch.getRow(curBatchPos0);

            int part = partitionHasher.eval(ctx, row);
            int idx =  part % outboxes.length;

            // TODO TODO: Bad: one slow output will not allow other proceed. How to fix that?
            if (!outboxes[idx].onRow(row)) {
                curBatchPos = curBatchPos0;

                return false;
            }
        }

        curBatch = null;
        curBatchPos = -1;
        curBatchRowCnt = -1;

        return true;
    }
}
