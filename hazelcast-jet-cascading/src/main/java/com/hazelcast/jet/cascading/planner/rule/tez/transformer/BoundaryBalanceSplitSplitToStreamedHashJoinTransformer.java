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

package com.hazelcast.jet.cascading.planner.rule.tez.transformer;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.rule.transformer.BoundaryElementFactory;
import cascading.flow.planner.rule.transformer.RuleInsertionTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.expression.BalanceSplitSplitToStreamedHashJoinExpression;

import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

/**
 * Inserts Boundary after split that joins back into a HashJoin by way of another split.
 * <p/>
 * this allows testGroupBySplitSplitGroupByJoin to pass
 */
public class BoundaryBalanceSplitSplitToStreamedHashJoinTransformer extends RuleInsertionTransformer {
    public BoundaryBalanceSplitSplitToStreamedHashJoinTransformer() {
        super(
                BalanceAssembly,
                new BalanceSplitSplitToStreamedHashJoinExpression(),
                ElementCapture.Secondary,
                BoundaryElementFactory.BOUNDARY_PIPE
        );
    }
}
