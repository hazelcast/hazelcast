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

package com.hazelcast.jet.cascading.planner.rule.tez.assertion;

import cascading.flow.planner.rule.RuleAssert;
import com.hazelcast.jet.cascading.planner.rule.tez.expression.DualStreamedAccumulatedExpression;

import static cascading.flow.planner.rule.PlanPhase.PostNodes;

/**
 * Throws a planner failure if a Tap, both accumulating and streamed, flows into a single merge within a given
 * pipeline.
 * <p/>
 * The optimization is to remove the streamed edge between tap and merge, but GraphFinder needs to support marking
 * edges.
 */
public class DualStreamedAccumulatedMergeNodeAssert extends RuleAssert {
    public DualStreamedAccumulatedMergeNodeAssert() {
        super(
                PostNodes,
                new DualStreamedAccumulatedExpression(),
                "may not merge accumulated and streamed in same pipeline: {Secondary}"
        );
    }
}
