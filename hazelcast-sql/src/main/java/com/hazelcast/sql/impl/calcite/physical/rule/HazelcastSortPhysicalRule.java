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

import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastRel;
import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastRootRel;
import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastSortRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.HazelcastDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastSortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastSortPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class HazelcastSortPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new HazelcastSortPhysicalRule();

    private HazelcastSortPhysicalRule() {
        super(
            RelOptRule.operand(HazelcastSortRel.class, RelOptRule.some(RelOptRule.operand(RelNode.class, RelOptRule.any()))),
            //RelOptRule.operand(HazelcastSortRel.class, HazelcastRel.LOGICAL, RelOptRule.any()),
            "HazelcastSortPhysicalRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        HazelcastSortRel sort = call.rel(0);
        RelNode input = sort.getInput();

        RelTraitSet inputTraits = RelTraitSet.createEmpty()
            .plus(HazelcastPhysicalRel.HAZELCAST_PHYSICAL)
            .plus(HazelcastDistributionTrait.ANY);

        HazelcastSortPhysicalRel newSort = new HazelcastSortPhysicalRel(
            sort.getCluster(),
            sort.getTraitSet().plus(HazelcastPhysicalRel.HAZELCAST_PHYSICAL).plus(HazelcastDistributionTrait.ANY),
            convert(input, inputTraits),
            sort.getCollation(),
            sort.offset,
            sort.fetch
        );

        RelTraitSet exchangeTraits = RelTraitSet.createEmpty()
            .plus(HazelcastPhysicalRel.HAZELCAST_PHYSICAL)
            .plus(HazelcastDistributionTrait.SINGLETON)
            .plus(sort.getCollation());

        HazelcastSortMergeExchangePhysicalRel newSortExchange = new HazelcastSortMergeExchangePhysicalRel(
            sort.getCluster(),
            exchangeTraits,
            newSort,
            sort.getCollation()
        );

        call.transformTo(newSortExchange);
    }
}
