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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;

import java.util.Collection;

/**
 * Abstract inbox implementation.
 */
public abstract class AbstractInbox extends AbstractMailbox implements InboundHandler {
    /** Initial size of batch queues. */
    protected static final int INITIAL_QUEUE_SIZE = 4;

    /** Number of enqueued batches. */
    protected int enqueuedBatches;

    /** Remaining remote sources. */
    private int remainingSources;

    /** Parent service. */
    private final QueryOperationHandler operationHandler;

    /** Backpressure control. */
    private final FlowControlState backpressure;

    protected AbstractInbox(
        QueryId queryId,
        int edgeId,
        int rowWidth,
        QueryOperationHandler operationHandler,
        int remainingSources,
        long maxMemory
    ) {
        super(queryId, edgeId, rowWidth);

        this.operationHandler = operationHandler;
        this.remainingSources = remainingSources;

        backpressure = new FlowControlState(maxMemory);
    }

    @Override
    public final void onBatch(InboundBatch batch, long remainingMemory) {
        onBatch0(batch);

        // Track done condition
        enqueuedBatches++;

        if (batch.isLast()) {
            remainingSources--;
        }

        // Track backpressure.
        backpressure.onBatchAdded(
            batch.getSenderId(),
            batch.isLast(),
            remainingMemory,
            batch.getBatch().getRowCount() * rowWidth
        );
    }

    protected abstract void onBatch0(InboundBatch batch);

    protected void onBatchPolled(InboundBatch batch) {
        if (batch == null) {
            return;
        }

        // Track done condition
        enqueuedBatches--;

        // Track backpressure.
        backpressure.onBatchRemoved(batch.getSenderId(), batch.isLast(), batch.getBatch().getRowCount() * rowWidth);
    }

    @Override
    public void sendFlowControl() {
        Collection<FlowControlStreamState> states = backpressure.getPending();

        if (states.isEmpty()) {
            return;
        }

        for (FlowControlStreamState state : states) {
            QueryFlowControlExchangeOperation operation =
                new QueryFlowControlExchangeOperation(queryId, edgeId, state.getLocalMemory());

            state.setShouldSend(false);

            boolean success = operationHandler.submit(state.getMemberId(), operation);

            if (!success) {
                throw HazelcastSqlException.error(SqlErrorCode.MEMBER_LEAVE,
                    "Failed to send control flow message to member: " + state.getMemberId());
            }
        }

        backpressure.clearPending();
    }

    /**
     * @return {@code True} if no more incoming batches are expected.
     */
    public boolean closed() {
        return enqueuedBatches == 0 && remainingSources == 0;
    }
}
