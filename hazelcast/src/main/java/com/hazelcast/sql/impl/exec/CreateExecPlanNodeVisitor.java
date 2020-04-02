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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.exec.agg.AggregateExec;
import com.hazelcast.sql.impl.exec.fetch.FetchExec;
import com.hazelcast.sql.impl.exec.index.MapIndexScanExec;
import com.hazelcast.sql.impl.exec.io.BroadcastSendExec;
import com.hazelcast.sql.impl.exec.io.ReceiveExec;
import com.hazelcast.sql.impl.exec.io.ReceiveSortMergeExec;
import com.hazelcast.sql.impl.exec.io.SingleSendExec;
import com.hazelcast.sql.impl.exec.io.UnicastSendExec;
import com.hazelcast.sql.impl.exec.join.HashJoinExec;
import com.hazelcast.sql.impl.exec.join.NestedLoopJoinExec;
import com.hazelcast.sql.impl.exec.root.RootExec;
import com.hazelcast.sql.impl.exec.io.mailbox.InboundHandler;
import com.hazelcast.sql.impl.exec.io.mailbox.Inbox;
import com.hazelcast.sql.impl.exec.io.mailbox.OutboundHandler;
import com.hazelcast.sql.impl.exec.io.mailbox.Outbox;
import com.hazelcast.sql.impl.exec.io.mailbox.StripedInbox;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.exec.io.flowcontrol.simple.SimpleFlowControl;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFragment;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.plan.node.AggregatePlanNode;
import com.hazelcast.sql.impl.plan.node.FetchPlanNode;
import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MaterializedInputPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.ReplicatedMapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.ReplicatedToPartitionedPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.SortPlanNode;
import com.hazelcast.sql.impl.plan.node.io.BroadcastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.io.EdgeAwarePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceiveSortMergePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;
import com.hazelcast.sql.impl.plan.node.io.UnicastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.join.HashJoinPlanNode;
import com.hazelcast.sql.impl.plan.node.join.NestedLoopJoinPlanNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

 /**
 * Visitor which builds an executor for every observed physical node.
 */
 @SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class CreateExecPlanNodeVisitor implements PlanNodeVisitor {
    /** Operation handler. */
    private final QueryOperationHandler operationHandler;

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Operation. */
    private final QueryExecuteOperation operation;

    /** Partitions owned by this data node. */
    private final PartitionIdSet localParts;

    /** Partition map. */
    private final Map<UUID, PartitionIdSet> partitionMap;

    /** Recommended outbox batch size in bytes. */
    private final int outboxBatchSize;

    /** Stack of elements to be merged. */
    private final ArrayList<Exec> stack = new ArrayList<>(1);

    /** Result. */
    private Exec exec;

    /** Inboxes. */
    private final Map<Integer, InboundHandler> inboxes = new HashMap<>();

    /** Outboxes. */
    private Map<Integer, Map<UUID, OutboundHandler>> outboxes = new HashMap<>();

    public CreateExecPlanNodeVisitor(
        QueryOperationHandler operationHandler,
        NodeEngine nodeEngine,
        QueryExecuteOperation operation,
        PartitionIdSet localParts,
        Map<UUID, PartitionIdSet> partitionMap,
        int outboxBatchSize
    ) {
        this.operationHandler = operationHandler;
        this.nodeEngine = nodeEngine;
        this.operation = operation;
        this.localParts = localParts;
        this.partitionMap = partitionMap;
        this.outboxBatchSize = outboxBatchSize;
    }


    @Override
    public void onRootNode(RootPlanNode node) {
        assert stack.size() == 1;

        exec = new RootExec(
            node.getId(),
            pop(),
            operation.getRootConsumer(),
            operation.getRootBatchSize()
        );
    }

    @Override
    public void onReceiveNode(ReceivePlanNode node) {
        // Navigate to sender exec and calculate total number of sender stripes.
        int edgeId = node.getEdgeId();

        int sendFragmentPos = operation.getOutboundEdgeMap().get(edgeId);
        QueryExecuteOperationFragment sendFragment = operation.getFragments().get(sendFragmentPos);

        int fragmentMemberCount = getFragmentMembers(sendFragment).size();

        // Create and register inbox.
        Inbox inbox = new Inbox(
            operation.getQueryId(),
            edgeId,
            sendFragment.getNode().getSchema().getEstimatedRowSize(),
            operationHandler,
            fragmentMemberCount,
            createFlowControl(edgeId)
        );

        inboxes.put(edgeId, inbox);

        // Instantiate executor and put it to stack.
        ReceiveExec res = new ReceiveExec(node.getId(), inbox);

        push(res);
    }

    @Override
    public void onReceiveSortMergeNode(ReceiveSortMergePlanNode node) {
        // Navigate to sender exec and calculate total number of sender stripes.
        int edgeId = node.getEdgeId();

        // Create and register inbox.
        int sendFragmentPos = operation.getOutboundEdgeMap().get(edgeId);
        QueryExecuteOperationFragment sendFragment = operation.getFragments().get(sendFragmentPos);

        StripedInbox inbox = new StripedInbox(
            operation.getQueryId(),
            edgeId,
            node.getSchema().getEstimatedRowSize(),
            operationHandler,
            getFragmentMembers(sendFragment),
            createFlowControl(edgeId)
        );

        inboxes.put(edgeId, inbox);

        // Instantiate executor and put it to stack.
        ReceiveSortMergeExec res = new ReceiveSortMergeExec(
            node.getId(),
            inbox,
            node.getExpressions(),
            node.getAscs(),
            node.getFetch(),
            node.getOffset()
        );

        push(res);
    }

    @Override
    public void onRootSendNode(RootSendPlanNode node) {
        Outbox[] outboxes = prepareOutboxes(node);

        assert outboxes.length == 1;

        exec = new SingleSendExec(node.getId(), pop(), outboxes[0]);
    }

    @Override
    public void onUnicastSendNode(UnicastSendPlanNode node) {
        Outbox[] outboxes = prepareOutboxes(node);

        if (outboxes.length == 1) {
            exec = new SingleSendExec(node.getId(), pop(), outboxes[0]);
        } else {
            int[] partitionOutboxIndexes = new int[localParts.getPartitionCount()];

            for (int outboxIndex = 0; outboxIndex < outboxes.length; outboxIndex++) {
                final int outboxIndex0 = outboxIndex;

                PartitionIdSet partitions = partitionMap.get(outboxes[outboxIndex0].getTargetMemberId());

                partitions.forEach((part) -> partitionOutboxIndexes[part] = outboxIndex0);
            }

            exec = new UnicastSendExec(
                node.getId(),
                pop(),
                outboxes,
                node.getPartitioner(),
                partitionOutboxIndexes
            );
        }
    }

    @Override
    public void onBroadcastSendNode(BroadcastSendPlanNode node) {
        Outbox[] outboxes = prepareOutboxes(node);

        if (outboxes.length == 1) {
            exec = new SingleSendExec(node.getId(), pop(), outboxes[0]);
        } else {
            exec = new BroadcastSendExec(node.getId(), pop(), outboxes);
        }
    }

    /**
     * Prepare outboxes for the given sender node.
     *
     * @param node Node.
     * @return Outboxes.
     */
    private Outbox[] prepareOutboxes(EdgeAwarePlanNode node) {
        int edgeId = node.getEdgeId();
        int rowWidth = node.getSchema().getEstimatedRowSize();

        int receiveFragmentPos = operation.getInboundEdgeMap().get(edgeId);
        QueryExecuteOperationFragment receiveFragment = operation.getFragments().get(receiveFragmentPos);
        Collection<UUID> receiveFragmentMemberIds = getFragmentMembers(receiveFragment);

        Outbox[] res = new Outbox[receiveFragmentMemberIds.size()];

        int i = 0;

        Map<UUID, OutboundHandler> edgeOutboxes = new HashMap<>();
        outboxes.put(edgeId, edgeOutboxes);

        for (UUID receiveMemberId : receiveFragmentMemberIds) {
            Outbox outbox = new Outbox(
                operation.getQueryId(),
                operationHandler,
                edgeId,
                rowWidth,
                receiveMemberId,
                outboxBatchSize,
                operation.getEdgeCreditMap().get(edgeId)
            );

            edgeOutboxes.put(receiveMemberId, outbox);

            res[i++] = outbox;
        }

        return res;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void onMapScanNode(MapScanPlanNode node) {
        Exec res;

        if (localParts == null) {
            res = new EmptyExec(node.getId());
        } else {
            String mapName = node.getMapName();

            MapProxyImpl map = (MapProxyImpl) nodeEngine.getHazelcastInstance().getMap(mapName);

            res = new MapScanExec(
                node.getId(),
                map,
                (InternalSerializationService) nodeEngine.getSerializationService(),
                localParts,
                node.getFieldNames(),
                node.getFieldTypes(),
                node.getProjects(),
                node.getFilter()
            );
        }

        push(res);
    }

    @Override
    public void onMapIndexScanNode(MapIndexScanPlanNode node) {
        Exec res;

        if (localParts == null) {
            res = new EmptyExec(node.getId());
        } else {
            String mapName = node.getMapName();

            MapProxyImpl<?, ?> map = (MapProxyImpl<?, ?>) nodeEngine.getHazelcastInstance().getMap(mapName);

            res = new MapIndexScanExec(
                node.getId(),
                map,
                (InternalSerializationService) nodeEngine.getSerializationService(),
                localParts,
                node.getFieldNames(),
                node.getFieldTypes(),
                node.getProjects(),
                node.getFilter(),
                node.getIndexName(),
                node.getIndexFilter()
            );
        }

        push(res);
    }

    @Override
    public void onReplicatedMapScanNode(ReplicatedMapScanPlanNode node) {
        String mapName = node.getMapName();

        ReplicatedMapProxy<?, ?> map = (ReplicatedMapProxy<?, ?>) nodeEngine.getHazelcastInstance().getReplicatedMap(mapName);

        Exec res = new ReplicatedMapScanExec(
            node.getId(),
            map,
            (InternalSerializationService) nodeEngine.getSerializationService(),
            node.getFieldNames(),
            node.getFieldTypes(),
            node.getProjects(),
            node.getFilter()
        );

        push(res);
    }

    @Override
    public void onSortNode(SortPlanNode node) {
        Exec res = new SortExec(
            node.getId(),
            pop(),
            node.getExpressions(),
            node.getAscs(),
            node.getFetch(),
            node.getOffset()
        );

        push(res);
    }

    @Override
    public void onProjectNode(ProjectPlanNode node) {
        Exec res = new ProjectExec(
            node.getId(),
            pop(),
            node.getProjects()
        );

        push(res);
    }

    @Override
    public void onFilterNode(FilterPlanNode node) {
        Exec res = new FilterExec(
            node.getId(),
            pop(),
            node.getFilter()
        );

        push(res);
    }

    @Override
    public void onAggregateNode(AggregatePlanNode node) {
        Exec res = new AggregateExec(
            node.getId(),
            pop(),
            node.getGroupKey(),
            node.getExpressions(),
            node.getSortedGroupKeySize()
        );

        push(res);
    }

    @Override
    public void onNestedLoopJoinNode(NestedLoopJoinPlanNode node) {
        Exec res = new NestedLoopJoinExec(
            node.getId(),
            pop(),
            pop(),
            node.getCondition(),
            node.isOuter(),
            node.isSemi(),
            node.getRightRowColumnCount()
        );

        push(res);
    }

    @Override
    public void onHashJoinNode(HashJoinPlanNode node) {
        Exec res = new HashJoinExec(
            node.getId(),
            pop(),
            pop(),
            node.getCondition(),
            node.getLeftHashKeys(),
            node.getRightHashKeys(),
            node.isOuter(),
            node.isSemi(),
            node.getRightRowColumnCount()
        );

        push(res);
    }

    @Override
    public void onMaterializedInputNode(MaterializedInputPlanNode node) {
        Exec upstream = pop();

        MaterializedInputExec res = new MaterializedInputExec(
            node.getId(),
            upstream
        );

        push(res);
    }

    @Override
    public void onReplicatedToPartitionedNode(ReplicatedToPartitionedPlanNode node) {
        Exec upstream = pop();

        ReplicatedToPartitionedExec res = new ReplicatedToPartitionedExec(
            node.getId(),
            upstream,
            node.getPartitioner(),
            localParts
        );

        push(res);
    }

    @Override
    public void onFetchNode(FetchPlanNode node) {
        Exec upstream = pop();

        FetchExec res = new FetchExec(
            node.getId(),
            upstream,
            node.getFetch(),
            node.getOffset()
        );

        push(res);
    }

    @Override
    public void onOtherNode(PlanNode node) {
        if (node instanceof CreateExecPlanNodeVisitorCallback) {
            ((CreateExecPlanNodeVisitorCallback) node).onVisit(this);
        } else {
            throw new UnsupportedOperationException("Unsupported node: " + node);
        }
    }

    public Exec getExec() {
        return exec;
    }

    public Map<Integer, InboundHandler> getInboxes() {
        return inboxes;
    }

    public Map<Integer, Map<UUID, OutboundHandler>> getOutboxes() {
         return outboxes;
    }

    /**
     * Public for testing purposes only.
     */
    public Exec pop() {
        return stack.remove(stack.size() - 1);
    }

    /**
     * Public for testing purposes only.
     */
    public void push(Exec exec) {
        stack.add(exec);
    }

    private FlowControl createFlowControl(int edgeId) {
        long maxMemory = operation.getEdgeCreditMap().get(edgeId);

        return new SimpleFlowControl(maxMemory);
    }

    private Collection<UUID> getFragmentMembers(QueryExecuteOperationFragment fragment) {
        if (fragment.getMapping() == QueryExecuteOperationFragmentMapping.EXPLICIT) {
            return fragment.getMemberIds();
        }

        assert fragment.getMapping() == QueryExecuteOperationFragmentMapping.DATA_MEMBERS;

        return partitionMap.keySet();
    }
}
