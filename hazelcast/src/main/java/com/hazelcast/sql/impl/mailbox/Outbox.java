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

package com.hazelcast.sql.impl.mailbox;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.operation.QueryOperationChannel;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Outbox which sends data to a single remote stripe.
 */
public class Outbox extends AbstractMailbox implements OutboundHandler {
    /** Minimum number of rows to be sent. */
    private static final int MIN_BATCH_FLUSH_THRESHOLD = 4;

    /** Operation handler. */
    private final QueryOperationHandler operationHandler;

    /** Target member ID. */
    private final UUID targetMemberId;

    /** Batch flush threshold. */
    private final int batchFlushTreshold;

    /** Channel to send operations through. */
    private QueryOperationChannel operationChannel;

    /** Pending rows. */
    private List<Row> rows;

    /** Amount of remote memory which is available at the moment. */
    private long remainingMemory;

    /** Whether all data was flushed. */
    private boolean flushedLast;

    public Outbox(
        QueryId queryId,
        QueryOperationHandler operationHandler,
        int edgeId,
        int rowWidth,
        UUID targetMemberId,
        int batchSize,
        long remainingMemory
    ) {
        super(queryId, edgeId, rowWidth);

        this.operationHandler = operationHandler;
        this.targetMemberId = targetMemberId;
        this.remainingMemory = remainingMemory;

        int batchFlushTreshold0 = batchSize / rowWidth;

        if (batchFlushTreshold0 < MIN_BATCH_FLUSH_THRESHOLD) {
            batchFlushTreshold0 = MIN_BATCH_FLUSH_THRESHOLD;
        }

        batchFlushTreshold = batchFlushTreshold0;
    }

    public void setup() {
        operationChannel = operationHandler.createChannel(targetMemberId);
    }

    public UUID getTargetMemberId() {
        return targetMemberId;
    }

    /**
     * Accept a row batch.
     *
     * @param rows Row batch.
     * @param position Position to start with.
     * @return Number of accepted rows.
     */
    public int onRowBatch(List<Row> rows, int position, boolean last) {
        // Number of rows in the batch.
        int batchRows = rows.size() - position;

        // Maximum number of rows which could be sent to remote node.
        int maxRowsToAccept = (int) (remainingMemory / rowWidth);

        if (maxRowsToAccept < 0) {
            maxRowsToAccept = Integer.MAX_VALUE;
        }

        // Number of rows which we are going to be accepted from the batch.
        int acceptedRows = Math.min(maxRowsToAccept, batchRows);

        if (acceptedRows != 0 || last) {
            // Collect rows.
            if (this.rows == null) {
                this.rows = new ArrayList<>(batchFlushTreshold);
            }

            for (int i = 0; i < acceptedRows; i++) {
                this.rows.add(rows.get(position + i));
            }

            // Apply backpressure.
            remainingMemory -= acceptedRows * rowWidth;

            // Flush if needed: when batch is too large, or if it was the last batch and it was fully accepted.
            boolean batchIsFull = this.rows.size() >= batchFlushTreshold || remainingMemory < rowWidth;
            boolean lastTransmit = last && acceptedRows == batchRows;

            if (batchIsFull || lastTransmit) {
                send(lastTransmit);
            }
        }

        return acceptedRows;
    }

    /**
     * Flushes queued rows if needed.
     *
     * @return {@code True} if all rows have been sent, {@code false} otherwise.
     */
    public boolean flushLastIfNeeded() {
        // No-op if already flushed.
        if (flushedLast) {
            return true;
        }

        // Otherwise we attempt to emulate empty batch sending and re-check whether last chunk has been flushed.
        onRowBatch(Collections.emptyList(), 0, true);

        return flushedLast;
    }

    @Override
    public void onFlowControl(long remainingMemory) {
        this.remainingMemory = remainingMemory;
    }

    /**
     * Send rows to target member.
     *
     * @param last Whether this is the last batch.
     */
    private void send(boolean last) {
        List<Row> rows0 = rows;

        if (rows0 == null) {
            rows0 = Collections.emptyList();
        }

        RowBatch batch = new ListRowBatch(rows0);

        QueryBatchExchangeOperation op = new QueryBatchExchangeOperation(queryId, edgeId, batch, last, remainingMemory);

        if (last) {
            flushedLast = true;
        }

        boolean success = operationChannel.submit(op);

        if (!success) {
            throw HazelcastSqlException.memberLeave(targetMemberId);
        }

        rows = null;
    }

    @Override
    public String toString() {
        return "Outbox {queryId=" + queryId + ", edgeId=" + edgeId + ", targetMemberId=" + targetMemberId + '}';
    }
}
