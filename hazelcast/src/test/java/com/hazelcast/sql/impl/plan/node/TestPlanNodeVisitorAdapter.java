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
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;
import com.hazelcast.sql.impl.plan.node.io.UnicastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.join.HashJoinPlanNode;
import com.hazelcast.sql.impl.plan.node.join.NestedLoopJoinPlanNode;

public abstract class TestPlanNodeVisitorAdapter implements PlanNodeVisitor {
    @Override
    public void onRootNode(RootPlanNode node) {
        // No-op.
    }

    @Override
    public void onReceiveNode(ReceivePlanNode node) {
        // No-op.
    }

    @Override
    public void onRootSendNode(RootSendPlanNode node) {
        // No-op.
    }

    @Override
    public void onUnicastSendNode(UnicastSendPlanNode node) {
        // No-op.
    }

    @Override
    public void onBroadcastSendNode(BroadcastSendPlanNode node) {
        // No-op.
    }

    @Override
    public void onMapScanNode(MapScanPlanNode node) {
        // No-op.
    }

    @Override
    public void onMapIndexScanNode(MapIndexScanPlanNode node) {
        // No-op.
    }

    @Override
    public void onReplicatedMapScanNode(ReplicatedMapScanPlanNode node) {
        // No-op.
    }

    @Override
    public void onSortNode(SortPlanNode node) {
        // No-op.
    }

    @Override
    public void onReceiveSortMergeNode(ReceiveSortMergePlanNode node) {
        // No-op.
    }

    @Override
    public void onProjectNode(ProjectPlanNode node) {
        // No-op.
    }

    @Override
    public void onFilterNode(FilterPlanNode node) {
        // No-op.
    }

    @Override
    public void onEmptyNode(EmptyPlanNode node) {
        // No-op.
    }

    @Override
    public void onAggregateNode(AggregatePlanNode node) {
        // No-op.
    }

    @Override
    public void onNestedLoopJoinNode(NestedLoopJoinPlanNode node) {
        // No-op.
    }

    @Override
    public void onHashJoinNode(HashJoinPlanNode node) {
        // No-op.
    }

    @Override
    public void onMaterializedInputNode(MaterializedInputPlanNode node) {
        // No-op.
    }

    @Override
    public void onReplicatedToPartitionedNode(ReplicatedToPartitionedPlanNode node) {
        // No-op.
    }

    @Override
    public void onFetchNode(FetchPlanNode node) {
        // No-op.
    }

    @Override
    public void onOtherNode(PlanNode node) {
        // No-op.
    }
}
