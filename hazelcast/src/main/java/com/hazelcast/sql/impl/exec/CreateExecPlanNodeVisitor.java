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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.exec.io.InboundHandler;
import com.hazelcast.sql.impl.exec.io.Inbox;
import com.hazelcast.sql.impl.exec.io.OutboundHandler;
import com.hazelcast.sql.impl.exec.io.Outbox;
import com.hazelcast.sql.impl.exec.io.ReceiveExec;
import com.hazelcast.sql.impl.exec.io.SendExec;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControlFactory;
import com.hazelcast.sql.impl.exec.root.RootExec;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFragment;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.io.EdgeAwarePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Visitor which builds an executor for every observed physical node.
 */
public class CreateExecPlanNodeVisitor implements PlanNodeVisitor {
    /** Operation handler. */
    private final QueryOperationHandler operationHandler;

    /** Node service provider. */
    private final NodeServiceProvider nodeServiceProvider;

    /** Serialization service. */
    private final InternalSerializationService serializationService;

    /** Local member ID. */
    private final UUID localMemberId;

    /** Operation. */
    private final QueryExecuteOperation operation;

    /** Factory to create flow control objects. */
    private final FlowControlFactory flowControlFactory;

    /** Partitions owned by this data node. */
    private final PartitionIdSet localParts;

    /** Recommended outbox batch size in bytes. */
    private final int outboxBatchSize;

    /** Stack of elements to be merged. */
    private final ArrayList<Exec> stack = new ArrayList<>(1);

    /** Result. */
    private Exec exec;

    /** Inboxes. */
    private final Map<Integer, InboundHandler> inboxes = new HashMap<>();

    /** Outboxes. */
    private final Map<Integer, Map<UUID, OutboundHandler>> outboxes = new HashMap<>();

    public CreateExecPlanNodeVisitor(
        QueryOperationHandler operationHandler,
        NodeServiceProvider nodeServiceProvider,
        InternalSerializationService serializationService,
        UUID localMemberId,
        QueryExecuteOperation operation,
        FlowControlFactory flowControlFactory,
        PartitionIdSet localParts,
        int outboxBatchSize
    ) {
        this.operationHandler = operationHandler;
        this.nodeServiceProvider = nodeServiceProvider;
        this.serializationService = serializationService;
        this.localMemberId = localMemberId;
        this.operation = operation;
        this.flowControlFactory = flowControlFactory;
        this.localParts = localParts;
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
            operationHandler,
            operation.getQueryId(),
            edgeId,
            node.getSchema().getEstimatedRowSize(),
            localMemberId,
            fragmentMemberCount,
            createFlowControl(edgeId)
        );

        inboxes.put(edgeId, inbox);

        // Instantiate executor and put it to stack.
        ReceiveExec res = new ReceiveExec(node.getId(), inbox);

        push(res);
    }

    @Override
    public void onRootSendNode(RootSendPlanNode node) {
        Outbox[] outboxes = prepareOutboxes(node);

        assert outboxes.length == 1;

        exec = new SendExec(node.getId(), pop(), outboxes[0]);
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
                operationHandler,
                operation.getQueryId(),
                edgeId,
                rowWidth,
                localMemberId,
                receiveMemberId,
                outboxBatchSize,
                operation.getEdgeInitialMemoryMap().get(edgeId)
            );

            edgeOutboxes.put(receiveMemberId, outbox);

            res[i++] = outbox;
        }

        return res;
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
    public void onMapScanNode(MapScanPlanNode node) {
        Exec res;

        if (localParts.isEmpty()) {
            res = new EmptyExec(node.getId());
        } else {
            String mapName = node.getMapName();

            MapContainer map = nodeServiceProvider.getMap(mapName);

            if (map == null) {
                res = new EmptyExec(node.getId());
            } else {
                res = new MapScanExec(
                    node.getId(),
                    map,
                    localParts,
                    node.getKeyDescriptor(),
                    node.getValueDescriptor(),
                    node.getFieldPaths(),
                    node.getFieldTypes(),
                    node.getProjects(),
                    node.getFilter(),
                    serializationService
                );
            }
        }

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

    /**
     * For testing only.
     */
    public void setExec(Exec exec) {
        this.exec = exec;
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
        long initialMemory = operation.getEdgeInitialMemoryMap().get(edgeId);

        return flowControlFactory.create(initialMemory);
    }

    private Collection<UUID> getFragmentMembers(QueryExecuteOperationFragment fragment) {
        if (fragment.getMapping() == QueryExecuteOperationFragmentMapping.EXPLICIT) {
            return fragment.getMemberIds();
        }

        assert fragment.getMapping() == QueryExecuteOperationFragmentMapping.DATA_MEMBERS;

        return operation.getPartitionMap().keySet();
    }
}
