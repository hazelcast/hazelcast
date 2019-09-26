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
 import com.hazelcast.sql.HazelcastSqlException;
 import com.hazelcast.sql.impl.QueryFragmentDescriptor;
 import com.hazelcast.sql.impl.exec.agg.LocalAggregateExec;
 import com.hazelcast.sql.impl.exec.join.LocalJoinExec;
 import com.hazelcast.sql.impl.mailbox.AbstractInbox;
 import com.hazelcast.sql.impl.mailbox.Outbox;
 import com.hazelcast.sql.impl.mailbox.SingleInbox;
 import com.hazelcast.sql.impl.mailbox.StripedInbox;
 import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
 import com.hazelcast.sql.impl.physical.CollocatedAggregatePhysicalNode;
 import com.hazelcast.sql.impl.physical.CollocatedJoinPhysicalNode;
 import com.hazelcast.sql.impl.physical.FilterPhysicalNode;
 import com.hazelcast.sql.impl.physical.MapScanPhysicalNode;
 import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;
 import com.hazelcast.sql.impl.physical.ProjectPhysicalNode;
 import com.hazelcast.sql.impl.physical.ReceivePhysicalNode;
 import com.hazelcast.sql.impl.physical.ReceiveSortMergePhysicalNode;
 import com.hazelcast.sql.impl.physical.ReplicatedMapScanPhysicalNode;
 import com.hazelcast.sql.impl.physical.RootPhysicalNode;
 import com.hazelcast.sql.impl.physical.SendPhysicalNode;
 import com.hazelcast.sql.impl.physical.SortPhysicalNode;

 import java.util.ArrayList;
 import java.util.Collection;
 import java.util.Collections;
 import java.util.List;
 import java.util.UUID;

 /**
 * Visitor which builds an executor for every observed physical node.
 */
public class CreateExecVisitor implements PhysicalNodeVisitor {
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

    /** Stack of elements to be merged. */
    private final ArrayList<Exec> stack = new ArrayList<>(1);

    /** Result. */
    private Exec exec;

    /** Inboxes. */
    private List<AbstractInbox> inboxes = new ArrayList<>(1);

    public CreateExecVisitor(
        NodeEngine nodeEngine,
        QueryExecuteOperation operation,
        QueryFragmentDescriptor currentFragment,
        Collection<UUID> dataMemberIds,
        PartitionIdSet localParts
    ) {
        this.nodeEngine = nodeEngine;
        this.operation = operation;
        this.currentFragment = currentFragment;
        this.dataMemberIds = dataMemberIds;
        this.localParts = localParts;
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
    public void onSendNode(SendPhysicalNode node) {
        assert stack.size() == 1;

        Outbox[] sendOutboxes;

        int senderFragmentDeploymentOffset = currentFragment.getAbsoluteDeploymentOffset(operation);

        int receiveFragmentPos = operation.getInboundEdgeMap().get(node.getEdgeId());
        QueryFragmentDescriptor receiveFragment = operation.getFragmentDescriptors().get(receiveFragmentPos);
        Collection<UUID> receiveFragmentMemberIds = receiveFragment.getFragmentMembers(dataMemberIds);
        int receiveFragmentDeploymentOffset = receiveFragment.getAbsoluteDeploymentOffset(operation);

        sendOutboxes = new Outbox[receiveFragmentMemberIds.size()];

        int idx = 0;

        for (UUID receiveMemberId : receiveFragmentMemberIds) {
            Outbox outbox = new Outbox(
                nodeEngine,
                operation.getQueryId(),
                node.getEdgeId(),
                senderFragmentDeploymentOffset,
                receiveMemberId,
                receiveFragmentDeploymentOffset,
                1024
            );

            sendOutboxes[idx++] = outbox;
        }

        exec = new SendExec(pop(), node.getPartitionHasher(), sendOutboxes);
    }

    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        Exec res;

        if (localParts == null)
            res = EmptyScanExec.INSTANCE;
        else {
            String mapName = node.getMapName();

            MapProxyImpl map = (MapProxyImpl)nodeEngine.getHazelcastInstance().getMap(mapName);

            if (map == null)
                throw new HazelcastSqlException(-1, "IMap doesn't exist: " + mapName);

            res = new MapScanExec(map, localParts, node.getProjections(), node.getFilter());
        }

        push(res);
    }

    @Override
    public void onReplicatedMapScanNode(ReplicatedMapScanPhysicalNode node) {
        Exec res = new ReplicatedMapScanExec(node.getMapName(), node.getProjections(), node.getFilter());

        push(res);
    }

    @Override
    public void onSortNode(SortPhysicalNode node) {
        Exec res = new LocalSortExec(pop(), node.getExpressions(), node.getAscs());

        push(res);
    }

    @Override
    public void onProjectNode(ProjectPhysicalNode node) {
        Exec res = new ProjectExec(pop(), node.getProjections());

        push(res);
    }

    @Override
    public void onFilterNode(FilterPhysicalNode node) {
        Exec res = new FilterExec(pop(), node.getCondition());

        push(res);
    }

    @Override
    public void onCollocatedAggregateNode(CollocatedAggregatePhysicalNode node) {
        Exec res = new LocalAggregateExec(pop(), node.getGroupKeySize(), node.getAccumulators(), node.isSorted());

        push(res);
    }

    @Override
    public void onCollocatedJoinNode(CollocatedJoinPhysicalNode node) {
        Exec res = new LocalJoinExec(
            pop(),
            pop(),
            node.getCondition()
        );

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
