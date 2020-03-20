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
import com.hazelcast.sql.impl.fragment.QueryFragmentContext;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.mailbox.MailboxBatch;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryAbstractExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.state.QueryState;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Query fragment executable which tracks whether it is scheduled for execution or not.
 */
public class QueryFragmentExecutable implements QueryFragmentScheduleCallback {

    private final QueryState state;
    private final List<Object> arguments;
    private final Exec exec;
    private final Map<Integer, AbstractInbox> inboxes;
    private final Map<Integer, Map<UUID, Outbox>> outboxes;
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
        QueryState state,
        List<Object> arguments,
        Exec exec,
        Map<Integer, AbstractInbox> inboxes,
        Map<Integer, Map<UUID, Outbox>> outboxes,
        QueryFragmentWorkerPool fragmentPool
    ) {
        this.state = state;
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

                    AbstractInbox inbox = inboxes.get(operation0.getEdgeId());
                    assert inbox != null;

                    MailboxBatch batch = new MailboxBatch(
                        operation0.getBatch(),
                        operation0.isLast(),
                        operation0.getCallerId()
                    );

                    inbox.onBatchReceived(batch, operation0.getRemainingMemory());
                } else {
                    assert operation instanceof QueryFlowControlExchangeOperation;

                    QueryFlowControlExchangeOperation operation0 = (QueryFlowControlExchangeOperation) operation;

                    Map<UUID, Outbox> edgeOutboxes = outboxes.get(operation0.getEdgeId());
                    assert edgeOutboxes != null;

                    Outbox outbox = edgeOutboxes.get(operation.getCallerId());
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
                for (AbstractInbox inbox : inboxes.values()) {
                    inbox.sendFlowControl();
                }
            }

            // If executor finished, notify the state.
            if (res == IterationResult.FETCHED_DONE) {
                completed = true;

                state.onFragmentFinished();
            }
        } catch (Exception e) {
            // Prevent subsequent invocations.
            completed = true;

            // Notify state about the exception to trigger cancel operation.
            state.cancel(e);
        }

        // Unschedule the fragment with double-check for new batches.
        unschedule();
    }

    @Override
    public void schedule() {
        // If the fragment is already scheduled, we do not need to do anything else, because executor will re-check queue state
        // before exiting.
        if (scheduled.get()) {
            return;
        }

        // Otherwise we schedule the fragment into the worker pool.
        if (scheduled.compareAndSet(false, true)) {
            fragmentPool.submit(this);
        }
    }

    /**
     * Unschedule the fragment.
     */
    private void unschedule() {
        // Unset the scheduled flag.
        scheduled.lazySet(false);

        // If new tasks arrived concurrently, reschedule the fragment again.
        if (!operations.isEmpty() && !completed) {
            schedule();
        }
    }

    private void setupExecutor() {
        if (initialized) {
            return;
        }

        try {
            exec.setup(new QueryFragmentContext(arguments, this, state));
        } finally {
            initialized = true;
        }
    }
}
