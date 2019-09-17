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

package com.hazelcast.sql.impl.worker.control;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.worker.AbstractWorker;
import com.hazelcast.sql.impl.worker.data.BatchDataTask;
import com.hazelcast.sql.impl.worker.data.DataThreadPool;
import com.hazelcast.sql.impl.worker.data.StartStripeDataTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Worker responsible for control tasks execution (start query, cancel query, handle membership changes and migrations).
 */
public class ControlWorker extends AbstractWorker<ControlTask> {
    /** Service. */
    private final SqlServiceImpl service;

    /** Data thread pool. */
    private final DataThreadPool dataThreadPool;

    private int lastDataThreadIdx = 0;

    /** Active queries. */
    private final Map<QueryId, QueryContext> queries = new HashMap<>();

    /** Pending batches (received before the query is deployed). */
    private final HashMap<QueryId, LinkedList<BatchDataTask>> pendingBatches = new HashMap<>();

    public ControlWorker(SqlServiceImpl service, NodeEngine nodeEngine, DataThreadPool dataThreadPool) {
        super(nodeEngine);

        this.service = service;
        this.dataThreadPool = dataThreadPool;
    }

    @Override
    protected void executeTask(ControlTask task) {
        if (task instanceof ExecuteControlTask)
            handleExecute((ExecuteControlTask)task);
        else if (task instanceof BatchDataTask)
            handleBatch((BatchDataTask)task);
    }

    @Override
    protected void onStop() {
        
    }

    private void handleExecute(ExecuteControlTask task) {
        // Prepare map of outbound edges to their counts. This is needed to understand when to close the receiver.
        Map<Integer, QueryFragment> sendFragmentMap = new HashMap<>();
        Map<Integer, QueryFragment> receiveFragmentMap = new HashMap<>();

        for (QueryFragment fragment : task.getFragments()) {
            Integer outboundEdge = fragment.getOutboundEdge();

            if (outboundEdge != null)
                sendFragmentMap.put(outboundEdge, fragment);

            if (fragment.getInboundEdges() != null) {
                for (Integer inboundEdge : fragment.getInboundEdges())
                    receiveFragmentMap.put(inboundEdge, fragment);
            }
        }

        QueryId queryId = task.getQueryId();

        // Fragment deployments.
        List<FragmentDeployment> fragmentDeployments = new ArrayList<>(2); // Root + non-root

        // This data structure maps edge stripes to real threads.
        Map<Integer, int[]> edgeToStripeMap = new HashMap<>();

        for (QueryFragment fragment : task.getFragments()) {
            // Skip fragments which should not execute on a node.
            if (!fragment.getMemberIds().contains(nodeEngine.getLocalMember().getUuid()))
                continue;

            List<StripeDeployment> stripeDeployments = new ArrayList<>(fragment.getParallelism());

            int[] stripeToThread = new int[fragment.getParallelism()];

            for (int i = 0; i < fragment.getParallelism(); i++) {
                ExecutorCreatePhysicalNodeVisitor visitor = new ExecutorCreatePhysicalNodeVisitor(
                    nodeEngine,
                    queryId,
                    nodeEngine.getPartitionService().getPartitionCount(),
                    task.getIds(),
                    task.getPartitionMapping().get(nodeEngine.getLocalMember().getUuid()),
                    sendFragmentMap,
                    receiveFragmentMap,
                    i,
                    fragment.getParallelism(),
                    task.getSeed()
                );

                fragment.getNode().visit(visitor);

                Exec exec = visitor.getExec();
                List<AbstractInbox> inboxes = visitor.getInboxes();
                List<Outbox> outboxes = visitor.getOutboxes();

                // Target thread is resolved *after* the executor is created, because it may depend in executor cost.
                int thread = lastDataThreadIdx++ % dataThreadPool.getStripeCount();

                for (AbstractInbox inbox : inboxes)
                    inbox.setThread(thread);

                for (Outbox outbox : outboxes)
                    outbox.setThread(thread);

                stripeToThread[i] = thread;

                stripeDeployments.add(new StripeDeployment(exec, i, thread, inboxes, outboxes));
            }

            // Prepare edge mapping.
            for (Integer edgeId : fragment.getInboundEdges())
                edgeToStripeMap.put(edgeId, stripeToThread);

            fragmentDeployments.add(new FragmentDeployment(stripeDeployments));
        }

        // Register context.
        QueryContext ctx = new QueryContext(
            nodeEngine,
            queryId,
            task.getArguments(),
            task.getRootConsumer(),
            edgeToStripeMap
        );

        queries.put(queryId, ctx);

        // Start query on executor.
        for (FragmentDeployment fragmentDeployment : fragmentDeployments) {
            for (StripeDeployment stripeDeployment :  fragmentDeployment.getStripes()) {
                stripeDeployment.initialize(ctx, fragmentDeployment);

                dataThreadPool.submit(new StartStripeDataTask(stripeDeployment));
            }
        }

        // Unwind pending batches which could have been received before query deployment.
        LinkedList<BatchDataTask> batches = pendingBatches.remove(queryId);

        if (batches != null) {
            for (BatchDataTask batch : batches) {
                int thread = ctx.getEdgeToStripeMap().get(batch.getEdgeId())[batch.getTargetStripe()];

                batch.setTargetThread(thread);

                service.onQueryBatchRequest(batch);
            }
        }
    }

    /**
     * Handle early batch when target stripe is not know in advance.
     *
     * @param task Task.
     */
    private void handleBatch(BatchDataTask task) {
        QueryId queryId = task.getQueryId();

        QueryContext ctx = queries.get(queryId);

        if (ctx == null) {
            // Context is missing. We either received early message before query is deployed locally, or
            // query is cancelled and we received a stale message. The latter will be cleaned with periodic
            // task.
            pendingBatches.computeIfAbsent(queryId, (k) -> new LinkedList<>()).add(task);
        }
        else {
            // Received unmapped batch. Resolve stripe and move to data thread.
            int thread = ctx.getEdgeToStripeMap().get(task.getEdgeId())[task.getTargetStripe()];

            task.setTargetThread(thread);

            service.onQueryBatchRequest(task);
        }
    }
}
