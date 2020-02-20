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

package com.hazelcast.sql.impl.physical.visitor;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.exec.EmptyScanExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.FilterExec;
import com.hazelcast.sql.impl.exec.MapScanExec;
import com.hazelcast.sql.impl.exec.MaterializedInputExec;
import com.hazelcast.sql.impl.exec.ProjectExec;
import com.hazelcast.sql.impl.exec.ReplicatedMapScanExec;
import com.hazelcast.sql.impl.exec.ReplicatedToPartitionedExec;
import com.hazelcast.sql.impl.exec.RootExec;
import com.hazelcast.sql.impl.exec.SortExec;
import com.hazelcast.sql.impl.exec.agg.AggregateExec;
import com.hazelcast.sql.impl.exec.index.MapIndexScanExec;
import com.hazelcast.sql.impl.exec.io.BroadcastSendExec;
import com.hazelcast.sql.impl.exec.io.ReceiveExec;
import com.hazelcast.sql.impl.exec.io.ReceiveSortMergeExec;
import com.hazelcast.sql.impl.exec.io.UnicastSendExec;
import com.hazelcast.sql.impl.exec.join.HashJoinExec;
import com.hazelcast.sql.impl.exec.join.NestedLoopJoinExec;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.mailbox.SingleInbox;
import com.hazelcast.sql.impl.mailbox.StripedInbox;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.physical.AggregatePhysicalNode;
import com.hazelcast.sql.impl.physical.FilterPhysicalNode;
import com.hazelcast.sql.impl.physical.MapIndexScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MaterializedInputPhysicalNode;
import com.hazelcast.sql.impl.physical.ProjectPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedMapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedToPartitionedPhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;
import com.hazelcast.sql.impl.physical.SortPhysicalNode;
import com.hazelcast.sql.impl.physical.io.BroadcastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.io.EdgeAwarePhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceivePhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceiveSortMergePhysicalNode;
import com.hazelcast.sql.impl.physical.io.UnicastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.join.HashJoinPhysicalNode;
import com.hazelcast.sql.impl.physical.join.NestedLoopJoinPhysicalNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

 /**
 * Visitor which builds an executor for every observed physical node.
 */
 @SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class CreateExecVisitor implements PhysicalNodeVisitor {
    // TODO: Understand how to calculate it properly. It should not be hardcoded.
    private static final int OUTBOX_BATCH_SIZE = 512 * 1024;

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Operation. */
    private final QueryExecuteOperation operation;

    /** Partitions owned by this data node. */
    private final PartitionIdSet localParts;

    /** Partition map. */
    private final Map<UUID, PartitionIdSet> partitionMap;

    /** Stack of elements to be merged. */
    private final ArrayList<Exec> stack = new ArrayList<>(1);

    /** Result. */
    private Exec exec;

    /** Inboxes. */
    private final Map<Integer, AbstractInbox> inboxes = new HashMap<>();

    /** Outboxes. */
    private Map<Integer, Map<UUID, Outbox>> outboxes = new HashMap<>();

    public CreateExecVisitor(
        NodeEngine nodeEngine,
        QueryExecuteOperation operation,
        PartitionIdSet localParts,
        Map<UUID, PartitionIdSet> partitionMap
    ) {
        this.nodeEngine = nodeEngine;
        this.operation = operation;
        this.localParts = localParts;
        this.partitionMap = partitionMap;
    }

    @Override
    public void onRootNode(RootPhysicalNode node) {
        assert stack.size() == 1;

        exec = new RootExec(
            node.getId(),
            pop()
        );
    }

    @Override
    public void onReceiveNode(ReceivePhysicalNode node) {
        // Navigate to sender exec and calculate total number of sender stripes.
        int edgeId = node.getEdgeId();

        int sendFragmentPos = operation.getOutboundEdgeMap().get(edgeId);
        QueryFragmentDescriptor sendFragment = operation.getFragmentDescriptors().get(sendFragmentPos);

        int fragmentMemberCount = sendFragment.getMappedMemberIds().size();

        // Create and register inbox.
        SingleInbox inbox = new SingleInbox(
            operation.getQueryId(),
            node.getEdgeId(),
            sendFragment.getNode().getSchema().getRowWidth(),
            nodeEngine.getSqlService(),
            fragmentMemberCount,
            operation.getEdgeCreditMap().get(edgeId)
        );

        inboxes.put(inbox.getEdgeId(), inbox);

        // Instantiate executor and put it to stack.
        ReceiveExec res = new ReceiveExec(node.getId(), inbox);

        push(res);
    }

    @Override
    public void onReceiveSortMergeNode(ReceiveSortMergePhysicalNode node) {
        // Navigate to sender exec and calculate total number of sender stripes.
        int edgeId = node.getEdgeId();

        // Create and register inbox.
        int sendFragmentPos = operation.getOutboundEdgeMap().get(edgeId);
        QueryFragmentDescriptor sendFragment = operation.getFragmentDescriptors().get(sendFragmentPos);

        StripedInbox inbox = new StripedInbox(
            operation.getQueryId(),
            edgeId,
            node.getSchema().getRowWidth(),
            nodeEngine.getSqlService(),
            sendFragment.getMappedMemberIds(),
            operation.getEdgeCreditMap().get(edgeId)
        );

        inboxes.put(inbox.getEdgeId(), inbox);

        // Instantiate executor and put it to stack.
        ReceiveSortMergeExec res = new ReceiveSortMergeExec(
            node.getId(),
            inbox,
            node.getExpressions(),
            node.getAscs()
        );

        push(res);
    }

    @Override
    public void onUnicastSendNode(UnicastSendPhysicalNode node) {
        Outbox[] outboxes = prepareOutboxes(node);

        int[] partitionOutboxIndexes = new int[localParts.getPartitionCount()];

        for (int outboxIndex = 0; outboxIndex < outboxes.length; outboxIndex++) {
            final int outboxIndex0 = outboxIndex;

            Outbox outbox = outboxes[outboxIndex0];

            UUID outboxMemberId = outbox.getTargetMemberId();

            PartitionIdSet partitions = partitionMap.get(outboxMemberId);

            partitions.forEach((part) -> partitionOutboxIndexes[part] = outboxIndex0);
        }

        exec = new UnicastSendExec(
            node.getId(),
            pop(),
            outboxes,
            node.getHashFunction(),
            partitionOutboxIndexes
        );
    }

    @Override
    public void onBroadcastSendNode(BroadcastSendPhysicalNode node) {
        Outbox[] outboxes = prepareOutboxes(node);

        exec = new BroadcastSendExec(
            node.getId(),
            pop(),
            outboxes
        );
    }

     /**
      * Prepare outboxes for the given sender node.
      *
      * @param node Node.
      * @return Outboxes.
      */
     private Outbox[] prepareOutboxes(EdgeAwarePhysicalNode node) {
         int edgeId = node.getEdgeId();
         int rowWidth = node.getSchema().getRowWidth();

         int receiveFragmentPos = operation.getInboundEdgeMap().get(edgeId);
         QueryFragmentDescriptor receiveFragment = operation.getFragmentDescriptors().get(receiveFragmentPos);
         Collection<UUID> receiveFragmentMemberIds = receiveFragment.getMappedMemberIds();

         Outbox[] res = new Outbox[receiveFragmentMemberIds.size()];

         int i = 0;

         Map<UUID, Outbox> edgeOutboxes = new HashMap<>();
         outboxes.put(edgeId, edgeOutboxes);

         for (UUID receiveMemberId : receiveFragmentMemberIds) {
             Outbox outbox = new Outbox(
                 operation.getQueryId(),
                 edgeId,
                 rowWidth,
                 receiveMemberId,
                 OUTBOX_BATCH_SIZE,
                 operation.getEdgeCreditMap().get(edgeId)
             );

             edgeOutboxes.put(receiveMemberId, outbox);

             res[i++] = outbox;
         }

         return res;
     }

    @SuppressWarnings("rawtypes")
    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        Exec res;

        if (localParts == null) {
            res = new EmptyScanExec(node.getId());
        } else {
            String mapName = node.getMapName();

            MapProxyImpl map = (MapProxyImpl) nodeEngine.getHazelcastInstance().getMap(mapName);

            res = new MapScanExec(
                node.getId(),
                map,
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
     public void onMapIndexScanNode(MapIndexScanPhysicalNode node) {
         Exec res;

         if (localParts == null) {
             res = new EmptyScanExec(node.getId());
         } else {
             String mapName = node.getMapName();

             MapProxyImpl<?, ?> map = (MapProxyImpl<?, ?>) nodeEngine.getHazelcastInstance().getMap(mapName);

             res = new MapIndexScanExec(
                 node.getId(),
                 map,
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
    public void onReplicatedMapScanNode(ReplicatedMapScanPhysicalNode node) {
        String mapName = node.getMapName();

        ReplicatedMapProxy<?, ?> map = (ReplicatedMapProxy<?, ?>) nodeEngine.getHazelcastInstance().getReplicatedMap(mapName);

        Exec res = new ReplicatedMapScanExec(
            node.getId(),
            map,
            node.getMapName(),
            node.getFieldNames(),
            node.getFieldTypes(),
            node.getProjects(),
            node.getFilter()
        );

        push(res);
    }

    @Override
    public void onSortNode(SortPhysicalNode node) {
        Exec res = new SortExec(
            node.getId(),
            pop(),
            node.getExpressions(),
            node.getAscs()
        );

        push(res);
    }

    @Override
    public void onProjectNode(ProjectPhysicalNode node) {
        Exec res = new ProjectExec(
            node.getId(),
            pop(),
            node.getProjects()
        );

        push(res);
    }

    @Override
    public void onFilterNode(FilterPhysicalNode node) {
        Exec res = new FilterExec(
            node.getId(),
            pop(),
            node.getFilter()
        );

        push(res);
    }

    @Override
    public void onAggregateNode(AggregatePhysicalNode node) {
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
    public void onNestedLoopJoinNode(NestedLoopJoinPhysicalNode node) {
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
    public void onHashJoinNode(HashJoinPhysicalNode node) {
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
    public void onMaterializedInputNode(MaterializedInputPhysicalNode node) {
        Exec upstream = pop();

        MaterializedInputExec res = new MaterializedInputExec(
            node.getId(),
            upstream
        );

        push(res);
    }

    @Override
    public void onReplicatedToPartitionedNode(ReplicatedToPartitionedPhysicalNode node) {
        Exec upstream = pop();

        ReplicatedToPartitionedExec res = new ReplicatedToPartitionedExec(
            node.getId(),
            upstream,
            node.getHashFunction(),
            localParts
        );

        push(res);
    }

    public Exec getExec() {
        return exec;
    }

    public Map<Integer, AbstractInbox> getInboxes() {
        return inboxes;
    }

    public Map<Integer, Map<UUID, Outbox>> getOutboxes() {
         return outboxes;
    }

    private Exec pop() {
        return stack.remove(stack.size() - 1);
    }

    private void push(Exec exec) {
        stack.add(exec);
    }
}
