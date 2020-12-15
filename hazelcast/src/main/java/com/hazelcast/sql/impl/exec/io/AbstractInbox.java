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

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.UUID;

/**
 * Abstract inbox implementation.
 */
public abstract class AbstractInbox extends AbstractMailbox implements InboundHandler {
    /** Comparator to order incoming batches based on their ordinals. */
    private static final Comparator<InboundBatch> BATCH_COMPARATOR = (b1, b2) -> Long.compare(b1.getOrdinal(), b2.getOrdinal());

    /** Number of enqueued batches. */
    protected int enqueuedBatches;

    /** Remaining active sources. */
    private int remainingStreams;

    /** Whether batches must be ordered according to their ordinals. */
    private final boolean ordered;

    /** Operation handler that is used to initiate operations. */
    private final QueryOperationHandler operationHandler;

    /** Implementation of the flow control interface. */
    private final FlowControl flowControl;

    /** If the inbox is ordered, contains the ordinal of the next expected batch. */
    private long expectedOrdinal;

    /** If the inbox is ordered, contains the batches with ordinals greater than then {@link #expectedOrdinal}. */
    private PriorityQueue<InboundBatch> pendingBatches;

    protected AbstractInbox(
        QueryOperationHandler operationHandler,
        QueryId queryId,
        int edgeId,
        boolean ordered,
        int rowWidth,
        UUID localMemberId,
        int remainingStreams,
        FlowControl flowControl
    ) {
        super(queryId, edgeId, rowWidth, localMemberId);

        this.ordered = ordered;
        this.operationHandler = operationHandler;
        this.remainingStreams = remainingStreams;
        this.flowControl = flowControl;
    }

    public void setup() {
        flowControl.setup(queryId, edgeId, localMemberId, operationHandler);
    }

    @Override
    public final void onBatch(InboundBatch batch, long remainingMemory) {
        // Increment the number of enqueued batches that is used as done condition
        enqueuedBatches++;

        // Track the backpressure
        flowControl.onBatchAdded(
            batch.getSenderId(),
            getBatchSize(batch),
            batch.isLast(),
            remainingMemory
        );

        if (!ordered) {
            // If the batch is not ordered, it could be processed immediately.
            processBatch(batch);
        } else {
            if (batch.getOrdinal() == expectedOrdinal) {
                // Process the current batch
                processBatch(batch);

                long expectedOrdinal0 = expectedOrdinal + 1;

                // Unwind pending batches, if any
                if (pendingBatches != null) {
                    while (true) {
                         InboundBatch nextBatch = pendingBatches.peek();

                         if (nextBatch != null && nextBatch.getOrdinal() == expectedOrdinal0) {
                             pendingBatches.poll();

                             processBatch(nextBatch);

                             expectedOrdinal0++;
                         } else {
                             break;
                         }
                    }
                }

                // Adjust the expected ordinal
                expectedOrdinal = expectedOrdinal0;
            } else {
                // Put the batch into the pending queue
                if (pendingBatches == null) {
                    pendingBatches = new PriorityQueue<>(1, BATCH_COMPARATOR);
                }

                pendingBatches.add(batch);
            }
        }
    }

    private void processBatch(InboundBatch batch) {
        onBatch0(batch);

        if (batch.isLast()) {
            remainingStreams--;
        }
    }

    protected abstract void onBatch0(InboundBatch batch);

    protected void onBatchPolled(InboundBatch batch) {
        if (batch == null) {
            return;
        }

        // Track done condition
        enqueuedBatches--;

        // Track backpressure.
        flowControl.onBatchRemoved(
            batch.getSenderId(),
            getBatchSize(batch),
            batch.isLast()
        );
    }

    @Override
    public void onFragmentExecutionCompleted() {
        flowControl.onFragmentExecutionCompleted();
    }

    /**
     * @return {@code true} if no more incoming batches are expected.
     */
    public boolean closed() {
        return enqueuedBatches == 0 && remainingStreams == 0;
    }

    public int getRemainingStreams() {
        return remainingStreams;
    }

    public FlowControl getFlowControl() {
        return flowControl;
    }

    private long getBatchSize(InboundBatch batch) {
        return (long) batch.getBatch().getRowCount() * rowWidth;
    }
}
