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

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.operation.QueryFlowControlOperation;

import java.util.Collection;

/**
 * Abstract inbox implementation.
 */
public abstract class AbstractInbox extends AbstractMailbox {
    /** Initial size of batch queues. */
    protected static final int INITIAL_QUEUE_SIZE = 4;

    /** Number of enqueued batches. */
    protected int enqueuedBatches;

    /** Remaining remote sources. */
    private int remainingSources;

    /** Parent service. */
    private final SqlServiceImpl service;

    /** Backpressure control. */
    private final InboxBackpressure backpressure;

    protected AbstractInbox(
        QueryId queryId,
        int edgeId,
        int rowWidth,
        SqlServiceImpl service,
        int remainingSources,
        long maxMemory
    ) {
        super(queryId, edgeId, rowWidth);

        this.service = service;
        this.remainingSources = remainingSources;

        backpressure = new InboxBackpressure(maxMemory);
    }

    /**
     * Handle batch arrival. Always invoked from the worker.
     */
    public void onBatchReceived(SendBatch batch) {
        onBatchReceived0(batch);

        // Track done condition
        enqueuedBatches++;

        if (batch.isLast()) {
            remainingSources--;
        }

        // Track backpressure.
        backpressure.onBatchAdded(
            batch.getSenderId(),
            batch.isLast(),
            batch.getRemainingMemory(),
            batch.getRows().size() * rowWidth
        );
    }

    protected abstract void onBatchReceived0(SendBatch batch);

    protected void onBatchPolled(SendBatch batch) {
        if (batch == null) {
            return;
        }

        // Track done condition
        enqueuedBatches--;

        // Track backpressure.
        backpressure.onBatchRemoved(batch.getSenderId(), batch.isLast(), batch.getRows().size() * rowWidth);
    }

    public void sendFlowControl() {
        Collection<InboxBackpressureState> states = backpressure.getPending();

        if (states.isEmpty()) {
            return;
        }

        long epochWatermark = service.getEpochWatermark();

        for (InboxBackpressureState state : states) {
            QueryFlowControlOperation operation = new QueryFlowControlOperation(
                epochWatermark,
                queryId,
                edgeId,
                state.getLocalMemory()
            );

            state.setShouldSend(false);

            service.sendRequest(operation, state.getMemberId());
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
