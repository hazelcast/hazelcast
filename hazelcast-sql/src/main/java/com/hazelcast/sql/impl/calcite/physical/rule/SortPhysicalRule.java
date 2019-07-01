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

package com.hazelcast.sql.impl.calcite.physical.rule;

import com.hazelcast.sql.impl.calcite.SqlCalciteConventions;
import com.hazelcast.sql.impl.calcite.logical.rel.SortLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.SortPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class SortPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new SortPhysicalRule();

    private SortPhysicalRule() {
        // TODO: Set LOGICAL convention.
        super(
            RelOptRule.operand(SortLogicalRel.class, RelOptRule.some(RelOptRule.operand(RelNode.class, RelOptRule.any()))),
            //RelOptRule.operand(SortLogicalRel.class, LogicalRel.LOGICAL, RelOptRule.any()),
            "SortPhysicalRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SortLogicalRel sort = call.rel(0);
        RelNode input = sort.getInput();

        RelTraitSet inputTraits = RelTraitSet.createEmpty()
            .plus(SqlCalciteConventions.HAZELCAST_PHYSICAL)
            .plus(PhysicalDistributionTrait.ANY);

        // Current implementation doesn't enforce any traits on child data sources.
        // Resulting tree is: Merge (SINGLETON) <- Scan (ANY) <- Input (ANY).
        // This way we do not produce addiotional exchanges.
        SortPhysicalRel newSort = new SortPhysicalRel(
            sort.getCluster(),
            sort.getTraitSet().plus(SqlCalciteConventions.HAZELCAST_PHYSICAL).plus(PhysicalDistributionTrait.ANY),
            convert(input, inputTraits),
            sort.getCollation(),
            sort.offset,
            sort.fetch
        );

        RelTraitSet exchangeTraits = RelTraitSet.createEmpty()
            .plus(SqlCalciteConventions.HAZELCAST_PHYSICAL)
            .plus(PhysicalDistributionTrait.SINGLETON)
            .plus(sort.getCollation());

        SortMergeExchangePhysicalRel newSortExchange = new SortMergeExchangePhysicalRel(
            sort.getCluster(),
            exchangeTraits,
            newSort,
            sort.getCollation()
        );

        call.transformTo(newSortExchange);
    }
}
