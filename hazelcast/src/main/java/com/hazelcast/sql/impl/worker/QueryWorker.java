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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.CreateExecVisitor;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.RootExec;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.mailbox.InboxKey;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.worker.task.AdvanceRootQueryTask;
import com.hazelcast.sql.impl.worker.task.ProcessBatchQueryTask;
import com.hazelcast.sql.impl.worker.task.QueryTask;
import com.hazelcast.sql.impl.worker.task.StartFragmentQueryTask;
import com.hazelcast.sql.impl.worker.task.StopQueryTask;
import com.hazelcast.util.EmptyStatement;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Query worker.
 */
// TODO: Proper exception handling
public class QueryWorker implements Runnable {
    /** Task queue. */
    private final LinkedBlockingDeque<QueryTask> queue = new LinkedBlockingDeque<>();

    /** Start latch. */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Node engine. */
    protected final NodeEngine nodeEngine;

    /** Logger. */
    protected final ILogger logger;

    /** Registered inboxes. */
    private final Map<InboxKey, AbstractInbox> inboxes = new HashMap<>();

    /** Pending batches (received before the query is deployed). */
    private final HashMap<QueryId, LinkedList<ProcessBatchQueryTask>> pendingBatches = new HashMap<>();

    /** Stop flag. */
    private boolean stop;

    protected QueryWorker(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;

        logger = nodeEngine.getLogger(this.getClass());
    }

    /**
     * Await worker start.
     */
    public void awaitStart() {
        try {
            startLatch.await();
        }
        catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();

            throw new HazelcastException("Failed to wait for worker start (thread has been interrupted).");
        }
    }

    /**
     * Offer a task.
     *
     * @param task Task.
     */
    public void offer(QueryTask task) {
        queue.offer(task);
    }

    /**
     * Stop the worker with a poison pill.
     */
    public void stop() {
        queue.offerFirst(StopQueryTask.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        startLatch.countDown();

        while (!stop) {
            try {
                QueryTask nextTask = queue.take();

                if (nextTask instanceof StopQueryTask) {
                    try {
                        onStop();
                    }
                    finally {
                        stop = true;
                    }
                }
                else
                    executeTask(nextTask);
            }
            catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
            catch (Exception e) {
                // Should never happen except of bugs. Write to log in case we missed some case (bug).
                logger.warning("Unexpected exception in SQL worker [threadName=" +
                    Thread.currentThread().getName() + ']', e);
            }
        }
    }

    /**
     * Execute the task.
     *
     * @param task Task.
     */
    private void executeTask(QueryTask task) {
        if (task instanceof StartFragmentQueryTask)
            handleStartFragmentTask((StartFragmentQueryTask)task);
        else if (task instanceof ProcessBatchQueryTask)
            handleProcessBatchTask((ProcessBatchQueryTask)task);
        else if (task instanceof AdvanceRootQueryTask)
            handleAdvanceRootTask((AdvanceRootQueryTask)task);
    }

    /**
     * Start execution of a new query.
     *
     * @param task Task.
     */
    private void handleStartFragmentTask(StartFragmentQueryTask task) {
        QueryExecuteOperation operation = task.getOperation();
        QueryFragmentDescriptor fragmentDescriptor = task.getFragment();

        // Create executor and mailboxes.
        CreateExecVisitor visitor = new CreateExecVisitor(
            nodeEngine,
            operation,
            fragmentDescriptor,
            operation.getPartitionMapping().keySet(),
            operation.getPartitionMapping().get(nodeEngine.getLocalMember().getUuid())
        );

        fragmentDescriptor.getNode().visit(visitor);

        Exec exec = visitor.getExec();

        // Setup inboxes.
        List<AbstractInbox> fragmentInboxes = visitor.getInboxes();

        for (AbstractInbox fragmentInbox : fragmentInboxes) {

            fragmentInbox.setExec(exec);

            inboxes.put(new InboxKey(operation.getQueryId(), fragmentInbox.getEdgeId()), fragmentInbox);
        }

        // Unwind pending batches if any.
        LinkedList<ProcessBatchQueryTask> pendingBatches0 = pendingBatches.remove(operation.getQueryId());

        if (pendingBatches0 != null) {
            for (ProcessBatchQueryTask pendingBatch : pendingBatches0) {
                offer(pendingBatch);
            }
        }

        // Setup and start executor.
        QueryContext fragmentContext = new QueryContext(
            nodeEngine,
            operation.getQueryId(),
            operation.getArguments(),
            this,
            task.getConsumer()
        );

        exec.setup(fragmentContext);

        advanceExecutor(exec);
    }

    /**
     * Process incoming batch.
     *
     * @param task Task descriptor.
     */
    private void handleProcessBatchTask(ProcessBatchQueryTask task) {
        // Locate the inbox.
        QueryId queryId = task.getQueryId();
        int edgeId = task.getEdgeId();

        InboxKey inboxKey = new InboxKey(queryId, edgeId);

        AbstractInbox inbox = inboxes.get(inboxKey);

        if (inbox == null) {
            // Inbox may be null in case the batch from another node arrived before query started, or after it was
            // cancelled. Put the batch to pending queue.
            pendingBatches.computeIfAbsent(queryId, (k) -> new LinkedList<>()).add(task);

            return;
        }

        // Otherwise feed the batch to the inbox and continue iteration.
        inbox.onBatch(task.getSourceMemberId(), task.getBatch());

        advanceExecutor(inbox.getExec());
    }

    /**
     * Try transferring data to user.
     *
     * @param task Task descriptor.
     */
    private void handleAdvanceRootTask(AdvanceRootQueryTask task) {
        RootExec root = task.getRootExec();

        advanceExecutor(root);
    }

    /**
     * Handle stop.
     */
    private void onStop() {
        // TODO: Clear all pending stuff
    }

    private void advanceExecutor(Exec exec) {
        exec.advance();

        // TODO: Cleanup context if finished.
    }
}
