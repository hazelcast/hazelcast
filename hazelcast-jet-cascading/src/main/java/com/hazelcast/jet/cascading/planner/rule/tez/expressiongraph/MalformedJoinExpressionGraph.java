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

package com.hazelcast.jet.cascading.planner.rule.tez.expressiongraph;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.pipe.HashJoin;
import cascading.pipe.Splice;

import static cascading.flow.planner.iso.expression.AndElementExpression.and;
import static cascading.flow.planner.iso.expression.NotElementExpression.not;

/**
 *
 */
public class MalformedJoinExpressionGraph extends ExpressionGraph {
    public MalformedJoinExpressionGraph() {
        super(
                SearchOrder.ReverseDepth,
                and(ElementCapture.Primary,
                        new FlowElementExpression(HashJoin.class) {
                            @Override
                            public boolean applies(PlannerContext plannerContext, ElementGraph elementGraph,
                                                   FlowElement flowElement) {
                                return super.applies(plannerContext, elementGraph, flowElement)
                                        && !((Splice) flowElement).isSelfJoin();
                            }
                        },
                        not(new FlowElementExpression(HashJoin.class, TypeExpression.Topo.SpliceOnly))
                )
        );
    }
}
