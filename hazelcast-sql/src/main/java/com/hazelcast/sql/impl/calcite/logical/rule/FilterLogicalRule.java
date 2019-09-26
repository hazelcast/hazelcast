/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.logical.rule;

import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.FilterLogicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;

// TODO: Investigate ability to merge scan and filter!
public final class FilterLogicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new FilterLogicalRule();

    private FilterLogicalRule() {
        super(
            RuleUtils.single(LogicalFilter.class, Convention.NONE),
            RelFactories.LOGICAL_BUILDER,
            FilterLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        RelNode input = filter.getInput();

        FilterLogicalRel newFilter = new FilterLogicalRel(
            filter.getCluster(),
            RuleUtils.toLogicalConvention(filter.getTraitSet()),
            RuleUtils.toLogicalInput(input),
            filter.getCondition()
        );

        call.transformTo(newFilter);
    }
}
