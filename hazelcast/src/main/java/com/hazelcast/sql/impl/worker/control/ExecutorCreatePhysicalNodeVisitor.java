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

 import com.hazelcast.map.impl.proxy.MapProxyImpl;
 import com.hazelcast.spi.impl.NodeEngine;
 import com.hazelcast.sql.HazelcastSqlException;
 import com.hazelcast.sql.impl.QueryFragment;
 import com.hazelcast.sql.impl.QueryId;
 import com.hazelcast.sql.impl.exec.agg.LocalAggregateExec;
 import com.hazelcast.sql.impl.exec.EmptyScanExec;
 import com.hazelcast.sql.impl.exec.Exec;
 import com.hazelcast.sql.impl.exec.FilterExec;
 import com.hazelcast.sql.impl.exec.MapScanExec;
 import com.hazelcast.sql.impl.exec.ProjectExec;
 import com.hazelcast.sql.impl.exec.ReceiveExec;
 import com.hazelcast.sql.impl.exec.ReceiveSortMergeExec;
 import com.hazelcast.sql.impl.exec.ReplicatedMapScanExec;
 import com.hazelcast.sql.impl.exec.RootExec;
 import com.hazelcast.sql.impl.exec.SendExec;
 import com.hazelcast.sql.impl.exec.LocalSortExec;
 import com.hazelcast.sql.impl.exec.join.LocalJoinExec;
 import com.hazelcast.sql.impl.mailbox.AbstractInbox;
 import com.hazelcast.sql.impl.mailbox.Outbox;
 import com.hazelcast.sql.impl.mailbox.SingleInbox;
 import com.hazelcast.sql.impl.mailbox.StripedInbox;
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
 import com.hazelcast.util.collection.PartitionIdSet;

 import java.util.ArrayList;
 import java.util.Collections;
 import java.util.List;
 import java.util.Map;

/**
 * Visitor which builds an executor for every observed physical node.
 */
public class ExecutorCreatePhysicalNodeVisitor implements PhysicalNodeVisitor {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Query ID. */
    private final QueryId queryId;

    /** Number of data partitions. */
    private final int partCnt;

    /** Member IDs. */
    private final List<String> memberIds;

    /** Partitions owned by this data node. */
    private final PartitionIdSet localParts;

    /** All participating fragments. */
    private final List<QueryFragment> fragments;

    /** Map from send (outbound) edge to it's fragment. */
    private final Map<Integer, Integer> outboundFragmentMap;

    /** Map from receive (inbound) edge to it's fragment. */
    private final Map<Integer, Integer> inboundFragmentMap;

    /** Stripe index. */
    private final int stripe;

    /** Number of stripes. */
    private final int stripeCnt;

    /** Seed. */
    private final int seed;

    /** Stack of elements to be merged. */
    private final ArrayList<Exec> stack = new ArrayList<>(1);

    /** Result. */
    private Exec exec;

    /** Inboxes. */
    private List<AbstractInbox> inboxes = new ArrayList<>(1);

    /** Outboxes. */
    private List<Outbox> outboxes = new ArrayList<>(1);

    public ExecutorCreatePhysicalNodeVisitor(
        NodeEngine nodeEngine,
        QueryId queryId,
        int partCnt,
        List<String> memberIds,
        PartitionIdSet localParts,
        List<QueryFragment> fragments,
        Map<Integer, Integer> outboundFragmentMap,
        Map<Integer, Integer> inboundFragmentMap,
        int stripe,
        int stripeCnt,
        int seed
    ) {
        this.nodeEngine = nodeEngine;
        this.queryId = queryId;
        this.partCnt = partCnt;
        this.memberIds = memberIds;
        this.localParts = localParts;
        this.fragments = fragments;
        this.outboundFragmentMap = outboundFragmentMap;
        this.inboundFragmentMap = inboundFragmentMap;
        this.stripe = stripe;
        this.stripeCnt = stripeCnt;
        this.seed = seed;
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

        QueryFragment sendFragment = fragments.get(outboundFragmentMap.get(edgeId));

        int remaining = sendFragment.getMemberIds().size() * sendFragment.getParallelism();

        // Create and register inbox.
        SingleInbox inbox = new SingleInbox(
            queryId,
            node.getEdgeId(),
            stripe,
            remaining
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
        QueryFragment sendFragment = fragments.get(outboundFragmentMap.get(edgeId));

        StripedInbox inbox = new StripedInbox(
            queryId,
            edgeId,
            stripe,
            sendFragment.getMemberIds(),
            sendFragment.getParallelism()
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

        // Partition by member count * parallelism.
        QueryFragment receiveFragment = fragments.get(inboundFragmentMap.get(node.getEdgeId()));

        int partCnt = receiveFragment.getMemberIds().size() * receiveFragment.getParallelism();

        sendOutboxes = new Outbox[partCnt];

        int idx = 0;

        for (String receiveMemberId : receiveFragment.getMemberIds()) {
            for (int j = 0; j < receiveFragment.getParallelism(); j++) {
                Outbox outbox = new Outbox(
                    node.getEdgeId(),
                    stripe,
                    queryId,
                    nodeEngine,
                    receiveMemberId,
                    1024,
                    j
                );

                sendOutboxes[idx++] = outbox;

                outboxes.add(outbox);
            }
        }

        exec = new SendExec(pop(), node.getPartitionHasher(), sendOutboxes);
    }

    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        PartitionIdSet stripeParts = new PartitionIdSet(partCnt);

        int stripePartsCnt = 0;

        if (localParts != null) {
            int ctr = 0;

            for (int i = 0; i < partCnt; i++) {
                if (localParts.contains(i)) {
                    if (ctr++ % stripeCnt == stripe) {
                        stripeParts.add(i);
                        stripePartsCnt++;
                    }
                }
            }
        }

        Exec res;

        if (stripePartsCnt == 0)
            res = EmptyScanExec.INSTANCE;
        else {
            String mapName = node.getMapName();

            MapProxyImpl map = (MapProxyImpl)nodeEngine.getHazelcastInstance().getMap(mapName);

            if (map == null)
                throw new HazelcastSqlException(-1, "IMap doesn't exist: " + mapName);

            res = new MapScanExec(map, stripeParts, node.getProjections(), node.getFilter());
        }

        push(res);
    }

    @Override
    public void onReplicatedMapScanNode(ReplicatedMapScanPhysicalNode node) {
        String memberId = memberIds.get(seed % memberIds.size());

        Exec res;

        if (nodeEngine.getLocalMember().getUuid().equals(memberId))
            res = new ReplicatedMapScanExec(node.getMapName(), node.getProjections(), node.getFilter());
        else
            res = EmptyScanExec.INSTANCE;

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

    public List<Outbox> getOutboxes() {
        return outboxes != null ? outboxes : Collections.emptyList();
    }

    private Exec pop() {
        return stack.remove(stack.size() - 1);
    }

    private void push(Exec exec) {
        stack.add(exec);
    }
}
