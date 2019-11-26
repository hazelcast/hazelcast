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

package com.hazelcast.sql.impl.physical.visitor;

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
import com.hazelcast.sql.impl.physical.io.ReceivePhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceiveSortMergePhysicalNode;
import com.hazelcast.sql.impl.physical.io.UnicastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.join.HashJoinPhysicalNode;
import com.hazelcast.sql.impl.physical.join.NestedLoopJoinPhysicalNode;

/**
 * Physical node visitor.
 */
public interface PhysicalNodeVisitor {
    void onRootNode(RootPhysicalNode node);
    void onReceiveNode(ReceivePhysicalNode node);
    void onUnicastSendNode(UnicastSendPhysicalNode node);
    void onBroadcastSendNode(BroadcastSendPhysicalNode node);
    void onMapScanNode(MapScanPhysicalNode node);
    void onMapIndexScanNode(MapIndexScanPhysicalNode node);
    void onReplicatedMapScanNode(ReplicatedMapScanPhysicalNode node);
    void onSortNode(SortPhysicalNode node);
    void onReceiveSortMergeNode(ReceiveSortMergePhysicalNode node);
    void onProjectNode(ProjectPhysicalNode node);
    void onFilterNode(FilterPhysicalNode node);
    void onAggregateNode(AggregatePhysicalNode node);
    void onNestedLoopJoinNode(NestedLoopJoinPhysicalNode node);
    void onHashJoinNode(HashJoinPhysicalNode node);
    void onMaterializedInputNode(MaterializedInputPhysicalNode node);
    void onReplicatedToPartitionedNode(ReplicatedToPartitionedPhysicalNode node);
}
