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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.AggregateLogicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;

// TODO: GROUP BY ("__key", ...) could be converted to plain projection because every group is guaranteed to be unique.
public final class AggregateLogicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new AggregateLogicalRule();

    private AggregateLogicalRule() {
        super(
            RuleUtils.single(LogicalAggregate.class, Convention.NONE),
            RelFactories.LOGICAL_BUILDER,
            AggregateLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        RelNode input = agg.getInput(0);

        if (agg.getGroupCount() > 1) {
            throw new HazelcastSqlException(-1, "Grouping sets are not supported.");
        }

        AggregateLogicalRel newAgg = new AggregateLogicalRel(
            agg.getCluster(),
            RuleUtils.toLogicalConvention(agg.getTraitSet()),
            RuleUtils.toLogicalInput(input),
            agg.indicator,
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList()
        );

        call.transformTo(newAgg);
    }
}
