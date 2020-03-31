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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.sql.impl.plan.node.io.BroadcastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceiveSortMergePlanNode;
import com.hazelcast.sql.impl.plan.node.io.UnicastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.join.HashJoinPlanNode;
import com.hazelcast.sql.impl.plan.node.join.NestedLoopJoinPlanNode;

/**
 * Physical node visitor.
 */
public interface PlanNodeVisitor {
    void onRootNode(RootPlanNode node);
    void onReceiveNode(ReceivePlanNode node);
    void onUnicastSendNode(UnicastSendPlanNode node);
    void onBroadcastSendNode(BroadcastSendPlanNode node);
    void onMapScanNode(MapScanPlanNode node);
    void onMapIndexScanNode(MapIndexScanPlanNode node);
    void onReplicatedMapScanNode(ReplicatedMapScanPlanNode node);
    void onSortNode(SortPlanNode node);
    void onReceiveSortMergeNode(ReceiveSortMergePlanNode node);
    void onProjectNode(ProjectPlanNode node);
    void onFilterNode(FilterPlanNode node);
    void onAggregateNode(AggregatePlanNode node);
    void onNestedLoopJoinNode(NestedLoopJoinPlanNode node);
    void onHashJoinNode(HashJoinPlanNode node);
    void onMaterializedInputNode(MaterializedInputPlanNode node);
    void onReplicatedToPartitionedNode(ReplicatedToPartitionedPlanNode node);
    void onFetchNode(FetchPlanNode node);

    /**
     * Callback for a node without special handlers. For testing only.
     *
     * @param node Node.
     */
    void onOtherNode(PlanNode node);
}
