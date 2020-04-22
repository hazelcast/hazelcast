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

import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;

/**
 * Plan node visitor. Typically used to convert the tree of plan nodes to another tree
 * (e.g. {@link com.hazelcast.sql.impl.exec.Exec}).
 * <p>
 * Normally we add a separate method for a new operator, to spot missing implementations during compilation. The method
 * {@link #onOtherNode(PlanNode)} allows for adding more operators without changes to the interface.
 * It should be used for testing only.
 * <p>
 * Visiting is expected to happen in the depth-first manner. Thus, nodes with inputs should delegate visit to children first
 * as explained in {@link PlanNode#visit(PlanNodeVisitor)}.
 */
public interface PlanNodeVisitor {
    void onRootNode(RootPlanNode node);
    void onReceiveNode(ReceivePlanNode node);
    void onRootSendNode(RootSendPlanNode node);
    void onProjectNode(ProjectPlanNode node);
    void onFilterNode(FilterPlanNode node);
    void onMapScanNode(MapScanPlanNode node);

    /**
     * Callback for a node without special handlers. For testing only.
     *
     * @param node Node.
     */
    void onOtherNode(PlanNode node);
}
