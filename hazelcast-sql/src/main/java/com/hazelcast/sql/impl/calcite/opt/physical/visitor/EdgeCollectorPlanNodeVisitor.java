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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.io.EdgeAwarePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor that collects edges of a plan fragment.
 */
public class EdgeCollectorPlanNodeVisitor implements PlanNodeVisitor {
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
    public void onRootNode(RootPlanNode node) {
        onNode(node);
    }

    @Override
    public void onReceiveNode(ReceivePlanNode node) {
        onNode(node);
    }

    @Override
    public void onRootSendNode(RootSendPlanNode node) {
        onNode(node);
    }

    @Override
    public void onMapScanNode(MapScanPlanNode node) {
        onNode(node);
    }

    @Override
    public void onProjectNode(ProjectPlanNode node) {
        onNode(node);
    }

    @Override
    public void onFilterNode(FilterPlanNode node) {
        onNode(node);
    }

    @Override
    public void onOtherNode(PlanNode node) {
        onNode(node);
    }

    private void onNode(PlanNode node) {
        if (node instanceof EdgeAwarePlanNode) {
            EdgeAwarePlanNode node0 = (EdgeAwarePlanNode) node;

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
