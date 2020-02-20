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

import com.hazelcast.cluster.Member;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryFragmentContext;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.operation.QueryBatchOperation;
import com.hazelcast.sql.impl.row.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Outbox which sends data to a single remote stripe.
 */
public class Outbox extends AbstractMailbox {

    private static final int MIN_BATCH_FLUSH_THRESHOLD = 4;

    /** Target member ID. */
    private final UUID targetMemberId;

    /** Batch flush threshold. */
    private final int batchFlushTreshold;

    /** SQL service. */
    private SqlServiceImpl sqlService;

    /** Target member. */
    private Member targetMember;

    /** Pending rows. */
    private List<Row> batch;

    /** Amount of remote memory which is available at the moment. */
    private long remainingMemory;

    /** Whether all data was flushed. */
    private boolean flushedLast;

    // TODO: Batch size must be measured in bytes!
    public Outbox(QueryId queryId, int edgeId, int rowWidth, UUID targetMemberId, int batchSize, long remainingMemory) {
        super(queryId, edgeId, rowWidth);

        this.targetMemberId = targetMemberId;
        this.remainingMemory = remainingMemory;

        int batchFlushTreshold0 = batchSize / rowWidth;

        if (batchFlushTreshold0 == 0) {
            batchFlushTreshold0 = MIN_BATCH_FLUSH_THRESHOLD;
        }

        batchFlushTreshold = batchFlushTreshold0;
    }

    public void setup(QueryFragmentContext context) {
        NodeEngine nodeEngine = context.getNodeEngine();

        sqlService = nodeEngine.getSqlService();

        targetMember = nodeEngine.getClusterService().getMember(targetMemberId);

        if (targetMember == null) {
            throw HazelcastSqlException.error(SqlErrorCode.MEMBER_LEAVE,
                "Outbox target member has left topology: " + targetMemberId);
        }
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

        // Current number of rows in buffer.
        int currentRows = batch != null ? batch.size() : 0;

        // Maximum number of rows which could be sent to remote node.
        int maxRowsToAccept = (int) (remainingMemory / rowWidth);

        if (maxRowsToAccept < 0) {
            maxRowsToAccept = Integer.MAX_VALUE;
        }

        // Number of rows which we are going to be accepted from the batch.
        int acceptedRows = Math.min(maxRowsToAccept, batchRows);

        if (acceptedRows != 0 || last) {
            // Collect rows.
            if (batch == null) {
                batch = new ArrayList<>(batchFlushTreshold);
            }

            for (int i = 0; i < acceptedRows; i++) {
                batch.add(rows.get(position + i));
            }

            // Apply backpressure.
            remainingMemory -= acceptedRows * rowWidth;

            // Flush if needed: when batch is too large, or if it was the last batch and it was fully accepted.
            boolean batchIsFull = batch.size() >= batchFlushTreshold || remainingMemory < rowWidth;
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

    public void onFlowControl(long remainingMemory) {
        this.remainingMemory = remainingMemory;
    }

    /**
     * Send rows to target member.
     *
     * @param last Whether this is the last batch.
     */
    private void send(boolean last) {
        List<Row> batch0 = batch;

        if (batch0 == null) {
            batch0 = Collections.emptyList();
        }

        QueryBatchOperation op = new QueryBatchOperation(
            sqlService.getEpochWatermark(),
            queryId,
            getEdgeId(),
            new SendBatch(batch0, remainingMemory, last)
        );

        if (last) {
            flushedLast = true;
        }

        boolean success = sqlService.sendRequest(op, targetMember.getAddress());

        if (!success) {
            throw HazelcastSqlException.error(SqlErrorCode.MEMBER_LEAVE,
                "Failed to send data batch to member: " + targetMemberId);
        }

        batch = null;
    }

    @Override
    public String toString() {
        return "Outbox {queryId=" + queryId
            + ", edgeId=" + getEdgeId()
            + ", targetMemberId=" + targetMemberId
            + '}';
    }
}
