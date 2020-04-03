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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.io.InboundHandler;
import com.hazelcast.sql.impl.exec.io.InboundBatch;
import com.hazelcast.sql.impl.exec.io.OutboundHandler;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryAbstractExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.state.QueryStateCallback;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Query fragment executable that advances the top-level operator, consumes data operations, and manages scheduling.
 */
public class QueryFragmentExecutable implements QueryFragmentScheduleCallback {

    private final QueryStateCallback stateCallback;
    private final List<Object> arguments;
    private final Exec exec;
    private final Map<Integer, InboundHandler> inboxes;
    private final Map<Integer, Map<UUID, OutboundHandler>> outboxes;
    private final QueryFragmentWorkerPool fragmentPool;

    /** Operations to be processed. */
    private final ConcurrentLinkedDeque<QueryAbstractExchangeOperation> operations = new ConcurrentLinkedDeque<>();

    /** Batch count which we used instead of batches.size() (which is O(N)) to prevent starvation. */
    private final AtomicInteger operationCount = new AtomicInteger();

    /** Schedule flag. */
    private final AtomicBoolean scheduled = new AtomicBoolean();

    /** Whether the fragment is initialized. */
    private volatile boolean initialized;

    /** Whether the fragment has completed. */
    private volatile boolean completed;

    public QueryFragmentExecutable(
        QueryStateCallback stateCallback,
        List<Object> arguments,
        Exec exec,
        Map<Integer, InboundHandler> inboxes,
        Map<Integer, Map<UUID, OutboundHandler>> outboxes,
        QueryFragmentWorkerPool fragmentPool
    ) {
        this.stateCallback = stateCallback;
        this.arguments = arguments;
        this.exec = exec;
        this.inboxes = inboxes;
        this.outboxes = outboxes;
        this.fragmentPool = fragmentPool;
    }

    public Collection<Integer> getInboxEdgeIds() {
        return inboxes.keySet();
    }

    public Collection<Integer> getOutboxEdgeIds() {
        return outboxes.keySet();
    }

    /**
     * Add operation to be processed.
     */
    public void addOperation(QueryAbstractExchangeOperation operation) {
        operationCount.incrementAndGet();
        operations.addLast(operation);
    }

    public void run() {
        try {
            if (completed) {
                return;
            }

            // Setup the executor if needed.
            setupExecutor();

            // Feed all batches to relevant inboxes first. Set the upper boundary on the number of batches to avoid
            // starvation when batches arrive quicker than we are able to process them.
            int maxOperationCount = operationCount.get();
            int processedBatchCount = 0;

            QueryOperation operation;

            while ((operation = operations.pollFirst()) != null) {
                if (operation instanceof QueryBatchExchangeOperation) {
                    QueryBatchExchangeOperation operation0 = (QueryBatchExchangeOperation) operation;

                    InboundHandler inbox = inboxes.get(operation0.getEdgeId());
                    assert inbox != null;

                    InboundBatch batch = new InboundBatch(
                        operation0.getBatch(),
                        operation0.isLast(),
                        operation0.getCallerId()
                    );

                    inbox.onBatch(batch, operation0.getRemainingMemory());
                } else {
                    assert operation instanceof QueryFlowControlExchangeOperation;

                    QueryFlowControlExchangeOperation operation0 = (QueryFlowControlExchangeOperation) operation;

                    Map<UUID, OutboundHandler> edgeOutboxes = outboxes.get(operation0.getEdgeId());
                    assert edgeOutboxes != null;

                    OutboundHandler outbox = edgeOutboxes.get(operation.getCallerId());
                    assert outbox != null;

                    outbox.onFlowControl(operation0.getRemainingMemory());
                }

                if (++processedBatchCount >= maxOperationCount) {
                    break;
                }
            }

            operationCount.addAndGet(-1 * processedBatchCount);

            IterationResult res = exec.advance();

            // Send flow control messages if needed.
            if (res != IterationResult.FETCHED_DONE) {
                for (InboundHandler inbox : inboxes.values()) {
                    inbox.onFragmentExecutionCompleted();
                }
            }

            // If executor finished, notify the state.
            if (res == IterationResult.FETCHED_DONE) {
                completed = true;

                stateCallback.onFragmentFinished();
            }
        } catch (Exception e) {
            // Prevent subsequent invocations.
            completed = true;

            // Notify state about the exception to trigger cancel operation.
            stateCallback.cancel(e);
        } finally {
            unscheduleOrReschedule();
        }
    }

    @Override
    public boolean schedule() {
        boolean res = !scheduled.get() && scheduled.compareAndSet(false, true);

        if (res) {
            submit();
        }

        return res;
    }

    /**
     * Unschedule the fragment.
     */
    private void unscheduleOrReschedule() {
        boolean completed0 = completed;

        // Check for new operations. If there are some, re-submit the fragment for execution immediately.
        if (!completed0 && !operations.isEmpty()) {
            // New operations arrived. Submit the fragment for execution again.
            submit();

            return;
        }

        // Otherwise, reset the "scheduled" flag to let other threads re-submit the fragment when needed.
        // Normal volatile write (seq-cst) is required here. Release semantics alone is not enough, because it will allow
        // the further check for pending operations to be reordered before the write.
        scheduled.set(false);

        // Double-check for new operations to prevent the race condition when another thread added the batch after we checked
        // for pending operations, but before we reset the "scheduled" flag.
        if (!completed0 && !operations.isEmpty()) {
            schedule();
        }
    }

    private void submit() {
        fragmentPool.submit(this);
    }

    private void setupExecutor() {
        if (initialized) {
            return;
        }

        try {
            exec.setup(new QueryFragmentContext(arguments, this, stateCallback));
        } finally {
            initialized = true;
        }
    }
}
