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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.mailbox.SendBatch;
import com.hazelcast.sql.impl.operation.QueryBatchOperation;
import com.hazelcast.sql.impl.operation.QueryDataExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryFlowControlOperation;
import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.worker.QueryWorkerPool;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Query fragment executable which tracks whether it is scheduled for execution or not.
 */
public class QueryFragmentExecutable {
    /** Global query state. */
    private final QueryState state;

    /** Exec for the given fragment. */
    private final Exec exec;

    /** Inboxes. */
    private final Map<Integer, AbstractInbox> inboxes;

    /** Outboxes. */
    private final Map<Integer, Map<UUID, Outbox>> outboxes;

    /** Context. */
    private final QueryFragmentContext fragmentContext;

    /** Operations to be processed. */
    private final ConcurrentLinkedDeque<QueryDataExchangeOperation> operations = new ConcurrentLinkedDeque<>();

    /** Batch count which we used instead aof batches.size() (which is O(N)) to prevent starvation. */
    private final AtomicInteger operationCount = new AtomicInteger();

    /** Schedule flag. */
    private final AtomicBoolean scheduled = new AtomicBoolean();

    /** Whether the fragment is initialized. */
    private volatile boolean initialized;

    /** Whether the fragment has completed. */
    private volatile boolean completed;

    public QueryFragmentExecutable(
        QueryState state,
        Exec exec,
        Map<Integer, AbstractInbox> inboxes,
        Map<Integer, Map<UUID, Outbox>> outboxes,
        QueryFragmentContext fragmentContext
    ) {
        this.state = state;
        this.exec = exec;
        this.inboxes = inboxes;
        this.outboxes = outboxes;
        this.fragmentContext = fragmentContext;
    }

    public Collection<Integer> getInboxEdgeIds() {
        return inboxes.keySet();
    }

    public Collection<Integer> getOutboxEdgeIds() {
        return outboxes.keySet();
    }

    /**
     * Schedule fragment execution.
     */
    public void schedule(QueryWorkerPool workerPool) {
        schedule0(workerPool);
    }

    /**
     * Add operation to be processed.
     */
    public void addOperation(QueryDataExchangeOperation operation) {
        operationCount.incrementAndGet();
        operations.addLast(operation);
    }

    public void run(QueryWorkerPool workerPool) {
        try {
            if (completed) {
                return;
            }

            // Setup the executor if needed.
            if (!initialized) {
                exec.setup(fragmentContext);

                initialized = true;
            }

            // Advance the executor.
            QueryFragmentContext.setCurrentContext(fragmentContext);

            try {
                // Feed all batches to relevant inboxes first. Set the upper boundary on the number of batches to avoid
                // starvation when batches arrive quicker than we are able to process them.
                int maxOperationCount = operationCount.get();
                int processedBatchCount = 0;

                QueryOperation operation;

                while ((operation = operations.pollFirst()) != null) {
                    if (operation instanceof QueryBatchOperation) {
                        QueryBatchOperation operation0 = (QueryBatchOperation) operation;

                        AbstractInbox inbox = inboxes.get(operation0.getEdgeId());
                        assert inbox != null;

                        SendBatch batch = operation0.getBatch();

                        batch.setSenderId(operation0.getCallerUuid());

                        inbox.onBatchReceived(batch);
                    } else {
                        assert operation instanceof QueryFlowControlOperation;

                        QueryFlowControlOperation operation0 = (QueryFlowControlOperation) operation;

                        Map<UUID, Outbox> edgeOutboxes = outboxes.get(operation0.getEdgeId());
                        assert edgeOutboxes != null;

                        Outbox outbox = edgeOutboxes.get(operation.getCallerUuid());
                        assert outbox != null;

                        outbox.onFlowControl(operation0.getRemainingMemory());
                    }

                    if (++processedBatchCount >= maxOperationCount) {
                        break;
                    }
                }

                operationCount.addAndGet(-1 * processedBatchCount);

                IterationResult res = exec.advance();

                // If executor finished, notify the state.
                if (res == IterationResult.FETCHED_DONE) {
                    completed = true;

                    state.onFragmentFinished();
                }
            } finally {
                QueryFragmentContext.setCurrentContext(null);
            }
        } catch (Exception e) {
            // Prevent subsequent invocations.
            completed = true;

            // Notify state about the exception to trigger cancel operation.
            state.cancel(e);
        }

        // Unschedule the fragment with double-check for new batches.
        unschedule(workerPool);
    }

    /**
     * Unschedule the fragment.
     */
    private void unschedule(QueryWorkerPool workerPool) {
        // Unset the scheduled flag.
        scheduled.lazySet(false);

        // If new tasks arrived concurrently, reschedule the fragment again.
        if (!operations.isEmpty() && !completed) {
            schedule0(workerPool);
        }
    }

    private void schedule0(QueryWorkerPool workerPool) {
        // If the fragment is already scheduled, we do not need to do anything else, because executor will re-check queue state
        // before exiting.
        if (scheduled.get()) {
            return;
        }

        // Otherwise we schedule the fragment into the worker pool.
        if (scheduled.compareAndSet(false, true)) {
            workerPool.submit(this);
        }
    }
}
