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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.physical.AggregatePhysicalNode;
import com.hazelcast.sql.impl.physical.FilterPhysicalNode;
import com.hazelcast.sql.impl.physical.MapIndexScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MaterializedInputPhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.visitor.PhysicalNodeVisitor;
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
import java.util.List;

public class EdgeCollectorPhysicalNodeVisitor implements PhysicalNodeVisitor {
    /** Outbound edge. */
    private Integer outboundEdge;

    /** Inbound edges. */
    private final List<Integer> inboundEdges = new ArrayList<>(1);

    public Integer getOutboundEdge() {
        return outboundEdge;
    }

    public List<Integer> getInboundEdges() {
        return inboundEdges;
    }

    @Override
    public void onRootNode(RootPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onReceiveNode(ReceivePhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onUnicastSendNode(UnicastSendPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onBroadcastSendNode(BroadcastSendPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onMapIndexScanNode(MapIndexScanPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onReplicatedMapScanNode(ReplicatedMapScanPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onSortNode(SortPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onReceiveSortMergeNode(ReceiveSortMergePhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onProjectNode(ProjectPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onFilterNode(FilterPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onAggregateNode(AggregatePhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onNestedLoopJoinNode(NestedLoopJoinPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onHashJoinNode(HashJoinPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onMaterializedInputNode(MaterializedInputPhysicalNode node) {
        onNode(node);
    }

    @Override
    public void onReplicatedToPartitionedNode(ReplicatedToPartitionedPhysicalNode node) {
        onNode(node);
    }

    private void onNode(PhysicalNode node) {
        if (node instanceof EdgeAwarePhysicalNode) {
            EdgeAwarePhysicalNode node0 = (EdgeAwarePhysicalNode) node;

            int edge = node0.getEdgeId();

            if (node0.isSender()) {
                assert outboundEdge == null;

                outboundEdge = edge;
            } else {
                inboundEdges.add(edge);
            }
        }
    }
}
