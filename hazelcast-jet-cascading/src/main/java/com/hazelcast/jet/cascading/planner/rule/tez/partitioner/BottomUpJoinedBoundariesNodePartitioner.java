/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.planner.rule.tez.partitioner;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.expressiongraph.NoGroupJoinMergeBoundaryTapExpressionGraph;
import cascading.flow.planner.rule.partitioner.ExpressionRulePartitioner;
import cascading.flow.stream.graph.IORole;
import com.hazelcast.jet.cascading.planner.rule.tez.expressiongraph.BottomUpConsecutiveBoundariesExpressionGraph;

import static cascading.flow.planner.rule.PlanPhase.PartitionNodes;

/**
 *
 */
public class BottomUpJoinedBoundariesNodePartitioner extends ExpressionRulePartitioner {
    public BottomUpJoinedBoundariesNodePartitioner() {
        super(
                PartitionNodes,

                new RuleExpression(
                        new NoGroupJoinMergeBoundaryTapExpressionGraph(),
                        new BottomUpConsecutiveBoundariesExpressionGraph()
                ),

                new ElementAnnotation(ElementCapture.Primary, IORole.sink)
        );
    }
}
