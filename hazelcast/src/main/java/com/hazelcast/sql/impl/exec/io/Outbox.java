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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryOperationChannel;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
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
    /** Operation handler. */
    private final QueryOperationHandler operationHandler;

    /** Target member ID. */
    private final UUID targetMemberId;

    /** Recommended batch size in bytes. The batch is sent when there is more enqueued data than this value. */
    private final int batchSize;

    /** Pending rows. */
    private List<Row> rows;

    /** Channel to send operations through. */
    private QueryOperationChannel operationChannel;

    /** Amount of remote memory which is available at the moment. */
    private long remainingMemory;

    public Outbox(
        QueryOperationHandler operationHandler,
        QueryId queryId,
        int edgeId,
        int rowWidth,
        UUID localMemberId,
        UUID targetMemberId,
        int batchSize,
        long remainingMemory
    ) {
        super(queryId, edgeId, rowWidth, localMemberId);

        this.operationHandler = operationHandler;
        this.targetMemberId = targetMemberId;
        this.batchSize = batchSize;
        this.remainingMemory = remainingMemory;
    }

    public void setup() {
        operationChannel = operationHandler.createChannel(localMemberId, targetMemberId);
    }

    public UUID getTargetMemberId() {
        return targetMemberId;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getRemainingMemory() {
        return remainingMemory;
    }

    /**
     * Accept a row batch.
     *
     * @param batch Batch.
     * @param last Whether this is the last batch.
     * @param position Position to start with.
     * @param qualifier Qualifier.
     * @return Sending position.
     */
    public int onRowBatch(RowBatch batch, boolean last, int position, OutboxSendQualifier qualifier) {
        // Get maximum number of rows which could be sent given the current memory constraints.
        int maxAcceptedRows = (int) (remainingMemory / rowWidth);
        int acceptedRows = 0;

        // Try to accept as much rows as possible.
        int currentPosition = position;

        for (; currentPosition < batch.getRowCount(); currentPosition++) {
            // Skip irrelevant rows.
            if (!qualifier.shouldSend(currentPosition)) {
                continue;
            }

            // Stop if we exhausted the space.
            if (acceptedRows == maxAcceptedRows) {
                break;
            }

            // Add pending row.
            if (rows == null) {
                rows = new ArrayList<>();
            }

            rows.add(batch.getRow(currentPosition));
            acceptedRows++;
        }

        // Adjust the remaining memory.
        remainingMemory = remainingMemory - (long) acceptedRows * rowWidth;

        // This is the very last transmission iff the whole last batch is consumed.
        boolean lastTransmit = last && currentPosition == batch.getRowCount();

        // The batch should be sent in the following cases:
        // 1) If this is the very last batch, even if it is empty - to signal the end of the stream
        // 2) If there are some data in the batch, and:
        //     2.1) There are more data than the recommended batch size
        //     2.2) Or we run out of memory, so that the remote end knows that we are low on memory, and the flow control is sent
        int batchRowCount = rows != null ? rows.size() : 0;

        boolean batchIsNotEmpty = batchRowCount > 0;
        boolean batchThresholdIsReached = batchRowCount * rowWidth >= batchSize;
        boolean cannotAcceptMoreRows = remainingMemory < rowWidth;

        boolean send = lastTransmit || (batchIsNotEmpty && (batchThresholdIsReached || cannotAcceptMoreRows));

        if (send) {
            send(lastTransmit);
        }

        return currentPosition;
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
        RowBatch batch = new ListRowBatch(rows != null ? rows : Collections.emptyList());

        assert batch.getRowCount() > 0 || last;

        QueryBatchExchangeOperation op = new QueryBatchExchangeOperation(
            queryId,
            edgeId,
            targetMemberId,
            batch,
            last,
            remainingMemory
        );

        boolean success = operationChannel.submit(op);

        if (!success) {
            throw QueryException.memberConnection(targetMemberId);
        }

        rows = null;
    }

    @Override
    public String toString() {
        return "Outbox {queryId=" + queryId + ", edgeId=" + edgeId + ", targetMemberId=" + targetMemberId + '}';
    }
}
