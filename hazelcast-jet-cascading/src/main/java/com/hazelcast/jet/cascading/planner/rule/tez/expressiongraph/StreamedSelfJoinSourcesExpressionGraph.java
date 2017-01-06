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

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.pipe.Boundary;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.OrElementExpression.or;

/**
 * Captures the source when the pipeline only has a single source and its streamed.
 */
public class StreamedSelfJoinSourcesExpressionGraph extends ExpressionGraph {
    public StreamedSelfJoinSourcesExpressionGraph() {
        super(SearchOrder.Depth, true);

        FlowElementExpression intermediate = new FlowElementExpression(HashJoin.class, TypeExpression.Topo.Linear);

        this.arc(
                or(ElementCapture.Primary,
                        new FlowElementExpression(Tap.class, TypeExpression.Topo.LinearOut),
                        new FlowElementExpression(Boundary.class, TypeExpression.Topo.LinearOut),
                        new FlowElementExpression(Group.class, TypeExpression.Topo.LinearOut)
                ),
                PathScopeExpression.ALL,
                intermediate
        );

        this.arc(
                intermediate,
                PathScopeExpression.ALL,
                or(
                        new FlowElementExpression(Tap.class),
                        new FlowElementExpression(Boundary.class),
                        new FlowElementExpression(Merge.class),
                        new FlowElementExpression(Group.class)
                )
        );
    }
}
