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

import cascading.flow.planner.iso.transformer.InsertionGraphTransformer;
import cascading.flow.planner.rule.transformer.BoundaryElementFactory;
import cascading.flow.planner.rule.transformer.RuleInsertionTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.expression.BalanceGroupSplitHashJoinExpression;

import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

/**
 *
 */
public class BoundaryBalanceGroupSplitHashJoinTransformer extends RuleInsertionTransformer {
    public BoundaryBalanceGroupSplitHashJoinTransformer() {
        super(
                BalanceAssembly,
                new BalanceGroupSplitHashJoinExpression(),
                BoundaryElementFactory.BOUNDARY_PIPE,
                InsertionGraphTransformer.Insertion.After
        );
    }
}
