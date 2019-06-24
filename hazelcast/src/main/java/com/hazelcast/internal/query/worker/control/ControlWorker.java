package com.hazelcast.internal.query.worker.control;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.QueryFragment;
import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.QueryService;
import com.hazelcast.internal.query.exec.Exec;
import com.hazelcast.internal.query.exec.Inbox;
import com.hazelcast.internal.query.exec.Outbox;
import com.hazelcast.internal.query.worker.AbstractWorker;
import com.hazelcast.internal.query.worker.data.BatchDataTask;
import com.hazelcast.internal.query.worker.data.DataThreadPool;
import com.hazelcast.internal.query.worker.data.StartStripeDataTask;
import com.hazelcast.util.collection.PartitionIdSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ControlWorker extends AbstractWorker<ControlTask> {

    private final QueryService service;
    private final DataThreadPool dataPool;

    // TODO: Use better algorithm for data worker distribution.
    private int lastDataThreadIdx = 0;

    /** Active queries. */
    private final Map<QueryId, QueryContext> queries = new HashMap<>();

    /** Pending batches. */
    private final HashMap<QueryId, List<BatchDataTask>> pendingBatches = new HashMap<>();

    public ControlWorker(QueryService service, DataThreadPool dataPool) {
        this.service = service;
        this.dataPool = dataPool;
    }

    @Override
    protected void executeTask(ControlTask task) {
        if (task instanceof ExecuteControlTask)
            handleExecute((ExecuteControlTask)task);
        else if (task instanceof BatchDataTask)
            handleBatch((BatchDataTask)task);
        else if (task instanceof CancelControlTask)
            handleCancel((CancelControlTask)task);

        // TODO: Other tasks.
    }

    @Override
    protected void onStop() {
        // TODO: Handle node stop
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

        // Build partition to member map for data partitioners.
        // TODO: Is it safe to call this locally (e.g. in case of cluster merge?)
        int partCnt = service.getNodeEngine().getPartitionService().getPartitionCount();

        MemberImpl[] partitionMap = new MemberImpl[partCnt];

        PartitionIdSet localParts = null;

        for (Map.Entry<String, PartitionIdSet> entry : task.getPartitionMapping().entrySet()) {
            String memberId = entry.getKey();

            // TODO: May be dead here, careful.
            MemberImpl member = service.getNodeEngine().getClusterService().getMember(memberId);

            for (int i = 0; i < partCnt; i++) {
                if (entry.getValue().contains(i))
                    partitionMap[i] = member;
            }

            // Preserve local partitions.
            if (member.localMember())
                localParts = entry.getValue();
        }

        // Fragment deployments.
        List<FragmentDeployment> fragmentDeployments = new ArrayList<>(2); // Root + non-root

        // This data structure maps edge stipes to real threads.
        Map<Integer, int[]> edgeToStripeMap = new HashMap<>();

        for (QueryFragment fragment : task.getFragments()) {
            // Skip fragments which should not execute on a node.
            if (!fragment.getMemberIds().contains(service.getNodeEngine().getLocalMember().getUuid()))
                continue;

            List<StripeDeployment> stripeDeployments = new ArrayList<>(fragment.getParallelism());

            int[] stripeToThread = new int[fragment.getParallelism()];

            for (int i = 0; i < fragment.getParallelism(); i++) {
                // TODO: Optimize (cache).
                List<Member> members = new ArrayList<>();

                for (String memberId : fragment.getMemberIds())
                    members.add(service.getNodeEngine().getClusterService().getMember(memberId));

                ExecutorCreatePhysicalNodeVisitor visitor = new ExecutorCreatePhysicalNodeVisitor(
                    service,
                    queryId,
                    partCnt,
                    localParts,
                    sendFragmentMap,
                    receiveFragmentMap
                );

                // TODO: Remove "reset" method.
                visitor.reset(i, fragment.getParallelism(), members);

                fragment.getNode().visit(visitor);

                Exec exec = visitor.getExec();
                List<Inbox> inboxes = visitor.getInboxes();
                List<Outbox> outboxes = visitor.getOutboxes();

                // Target thread is resolved *after* the executor is created, because it may depend in executor cost.
                int thread = lastDataThreadIdx++ % dataPool.getStripeCount();

                for (Inbox inbox : inboxes)
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
            service,
            queryId,
            task.getArguments(),
            task.getRootConsumer(),
            fragmentDeployments,
            edgeToStripeMap
        );

        // TODO: Cancel "antimatter".
        queries.put(queryId, ctx);

        // TODO: Start query in executor.
        for (FragmentDeployment fragmentDeployment : ctx.getFragmentDeployments()) {
            for (StripeDeployment stripeDeployment :  fragmentDeployment.getStripes()) {
                stripeDeployment.initialize(ctx, fragmentDeployment);

                dataPool.submit(new StartStripeDataTask(stripeDeployment));
            }
        }

        // Unwind pending batches.
        List<BatchDataTask> batches = pendingBatches.remove(queryId);

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

            // TODO: Linked list?
            pendingBatches.computeIfAbsent(queryId, (k) -> new LinkedList<>()).add(task);

            // TODO: Cleanup timeout.
        }
        else {
            // Received unmapped batch. Resolve stripe and move to data thread.
            int thread = ctx.getEdgeToStripeMap().get(task.getEdgeId())[task.getTargetStripe()];

            task.setTargetThread(thread);

            service.onQueryBatchRequest(task);
        }
    }

    private void handleCancel(CancelControlTask task) {
        // TODO
    }

    private void handleMember(MemberLeaveControlTask task) {
        // TODO
    }
}
