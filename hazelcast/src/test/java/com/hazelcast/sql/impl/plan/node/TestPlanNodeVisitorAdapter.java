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
    public void onProjectNode(ProjectPlanNode node) {
        // No-op.
    }

    @Override
    public void onFilterNode(FilterPlanNode node) {
        // No-op.
    }

    @Override
    public void onMapScanNode(MapScanPlanNode node) {
        // No-op.
    }

    @Override
    public void onOtherNode(PlanNode node) {
        // No-op.
    }
}
