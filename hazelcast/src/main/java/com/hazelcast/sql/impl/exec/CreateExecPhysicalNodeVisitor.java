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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.exec.agg.LocalAggregateExec;
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
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;
import com.hazelcast.sql.impl.physical.ProjectPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedMapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedToPartitionedPhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;
import com.hazelcast.sql.impl.physical.SortPhysicalNode;
import com.hazelcast.sql.impl.physical.io.BroadcastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceivePhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceiveSortMergePhysicalNode;
import com.hazelcast.sql.impl.physical.io.UnicastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.join.HashJoinPhysicalNode;
import com.hazelcast.sql.impl.physical.join.NestedLoopJoinPhysicalNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

 /**
 * Visitor which builds an executor for every observed physical node.
 */
 @SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class CreateExecPhysicalNodeVisitor implements PhysicalNodeVisitor {
    // TODO: Understand how to calculate it properly. It should not be hardcoded.
    private static final int OUTBOX_BATCH_SIZE = 1024;

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Operation. */
    private final QueryExecuteOperation operation;

    /** Current fragment. */
    private final QueryFragmentDescriptor currentFragment;

    /** Member IDs. */
    private final Collection<UUID> dataMemberIds;

    /** Partitions owned by this data node. */
    private final PartitionIdSet localParts;

    /** Partition map. */
    private final Map<UUID, PartitionIdSet> partitionMap;

    /** Stack of elements to be merged. */
    private final ArrayList<Exec> stack = new ArrayList<>(1);

    /** Result. */
    private Exec exec;

    /** Inboxes. */
    private List<AbstractInbox> inboxes = new ArrayList<>(1);

    public CreateExecPhysicalNodeVisitor(
        NodeEngine nodeEngine,
        QueryExecuteOperation operation,
        QueryFragmentDescriptor currentFragment,
        Collection<UUID> dataMemberIds,
        PartitionIdSet localParts,
        Map<UUID, PartitionIdSet> partitionMap
    ) {
        this.nodeEngine = nodeEngine;
        this.operation = operation;
        this.currentFragment = currentFragment;
        this.dataMemberIds = dataMemberIds;
        this.localParts = localParts;
        this.partitionMap = partitionMap;
    }

    @Override
    public void onRootNode(RootPhysicalNode node) {
        assert stack.size() == 1;

        exec = new RootExec(pop());
    }

    @Override
    public void onReceiveNode(ReceivePhysicalNode node) {
        // Navigate to sender exec and calculate total number of sender stripes.
        int edgeId = node.getEdgeId();

        int sendFragmentPos = operation.getOutboundEdgeMap().get(edgeId);
        QueryFragmentDescriptor sendFragment = operation.getFragmentDescriptors().get(sendFragmentPos);

        int fragmentMemberCount = sendFragment.getFragmentMembers(dataMemberIds).size();

        // Create and register inbox.
        SingleInbox inbox = new SingleInbox(
            operation.getQueryId(),
            node.getEdgeId(),
            fragmentMemberCount
        );

        inboxes.add(inbox);

        // Instantiate executor and put it to stack.
        ReceiveExec res = new ReceiveExec(inbox);

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
            sendFragment.getFragmentMembers(dataMemberIds)
        );

        inboxes.add(inbox);

        // Instantiate executor and put it to stack.
        ReceiveSortMergeExec res = new ReceiveSortMergeExec(
            inbox,
            node.getExpressions(),
            node.getAscs()
        );

        push(res);
    }

    @Override
    public void onUnicastSendNode(UnicastSendPhysicalNode node) {
        Outbox[] outboxes = prepareOutboxes(node.getEdgeId());

        int[] partitionOutboxIndexes = new int[localParts.getPartitionCount()];

        for (int outboxIndex = 0; outboxIndex < outboxes.length; outboxIndex++) {
            final int outboxIndex0 = outboxIndex;

            Outbox outbox = outboxes[outboxIndex0];

            UUID outboxMemberId = outbox.getTargetMemberId();

            PartitionIdSet partitions = partitionMap.get(outboxMemberId);

            partitions.forEach((part) -> partitionOutboxIndexes[part] = outboxIndex0);
        }

        exec = new UnicastSendExec(pop(), outboxes, node.getHashFunction(), partitionOutboxIndexes);
    }

    @Override
    public void onBroadcastSendNode(BroadcastSendPhysicalNode node) {
        Outbox[] outboxes = prepareOutboxes(node.getEdgeId());

        exec = new BroadcastSendExec(pop(), outboxes);
    }

     /**
      * Prepare outboxes for the given edge ID.
      *
      * @param edgeId Edge ID.
      * @return Outboxes.
      */
     private Outbox[] prepareOutboxes(int edgeId) {
         int senderFragmentDeploymentOffset = currentFragment.getAbsoluteDeploymentOffset(operation);

         int receiveFragmentPos = operation.getInboundEdgeMap().get(edgeId);
         QueryFragmentDescriptor receiveFragment = operation.getFragmentDescriptors().get(receiveFragmentPos);
         Collection<UUID> receiveFragmentMemberIds = receiveFragment.getFragmentMembers(dataMemberIds);
         int receiveFragmentDeploymentOffset = receiveFragment.getAbsoluteDeploymentOffset(operation);

         Outbox[] outboxes = new Outbox[receiveFragmentMemberIds.size()];

         int idx = 0;

         for (UUID receiveMemberId : receiveFragmentMemberIds) {
             Outbox outbox = new Outbox(
                 nodeEngine,
                 operation.getQueryId(),
                 edgeId,
                 senderFragmentDeploymentOffset,
                 receiveMemberId,
                 receiveFragmentDeploymentOffset,
                 OUTBOX_BATCH_SIZE
             );

             outboxes[idx++] = outbox;
         }

         return outboxes;
     }

    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        Exec res;

        if (localParts == null) {
            res = EmptyScanExec.INSTANCE;
        } else {
            String mapName = node.getMapName();

            MapProxyImpl map = (MapProxyImpl) nodeEngine.getHazelcastInstance().getMap(mapName);

            res = new MapScanExec(
                map,
                localParts,
                node.getFieldNames(),
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
             res = EmptyScanExec.INSTANCE;
         } else {
             String mapName = node.getMapName();

             MapProxyImpl map = (MapProxyImpl) nodeEngine.getHazelcastInstance().getMap(mapName);

             res = new MapIndexScanExec(
                 map,
                 localParts,
                 node.getFieldNames(),
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
        Exec res = new ReplicatedMapScanExec(
            node.getMapName(),
            node.getFieldNames(),
            node.getProjects(),
            node.getFilter()
        );

        push(res);
    }

    @Override
    public void onSortNode(SortPhysicalNode node) {
        Exec res = new LocalSortExec(pop(), node.getExpressions(), node.getAscs());

        push(res);
    }

    @Override
    public void onProjectNode(ProjectPhysicalNode node) {
        Exec res = new ProjectExec(pop(), node.getProjects());

        push(res);
    }

    @Override
    public void onFilterNode(FilterPhysicalNode node) {
        Exec res = new FilterExec(pop(), node.getFilter());

        push(res);
    }

    @Override
    public void onAggregateNode(AggregatePhysicalNode node) {
        Exec res = new LocalAggregateExec(pop(), node.getGroupKey(), node.getExpressions(), node.getSortedGroupKeySize());

        push(res);
    }

    @Override
    public void onNestedLoopJoinNode(NestedLoopJoinPhysicalNode node) {
        Exec res = new NestedLoopJoinExec(
            pop(),
            pop(),
            node.getCondition()
        );

        push(res);
    }

    @Override
    public void onHashJoinNode(HashJoinPhysicalNode node) {
        Exec res = new HashJoinExec(
            pop(),
            pop(),
            node.getCondition(),
            node.getLeftHashKeys(),
            node.getRightHashKeys(),
            node.isOuter(),
            node.getRightRowColumnCount()
        );

        push(res);
    }

    @Override
    public void onMaterializedInputNode(MaterializedInputPhysicalNode node) {
        Exec upstream = pop();

        MaterializedInputExec res = new MaterializedInputExec(upstream);

        push(res);
    }

    @Override
    public void onReplicatedToPartitionedNode(ReplicatedToPartitionedPhysicalNode node) {
        Exec upstream = pop();

        ReplicatedToPartitionedExec res = new ReplicatedToPartitionedExec(upstream, node.getHashFunction(), localParts);

        push(res);
    }

    public Exec getExec() {
        return exec;
    }

    public List<AbstractInbox> getInboxes() {
        return inboxes != null ? inboxes : Collections.emptyList();
    }

    private Exec pop() {
        return stack.remove(stack.size() - 1);
    }

    private void push(Exec exec) {
        stack.add(exec);
    }
}
