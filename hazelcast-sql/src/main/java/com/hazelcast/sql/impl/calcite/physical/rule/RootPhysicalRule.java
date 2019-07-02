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

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.logical.rel.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.RootPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class RootPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new RootPhysicalRule();

    private RootPhysicalRule() {
        // TODO: Set LOGICAL convention.
        super(
            RelOptRule.operand(RootLogicalRel.class, RelOptRule.some(RelOptRule.operand(RelNode.class, RelOptRule.any()))),
            "RootPhysicalRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RootLogicalRel root = call.rel(0);
        RelNode input = call.rel(1);

        RelTraitSet traits = input.getTraitSet()
            .plus(HazelcastConventions.HAZELCAST_PHYSICAL)
            .plus(PhysicalDistributionTrait.SINGLETON);

        final RelNode convertedInput = convert(input, traits.simplify());

        RootPhysicalRel rootPhysical = new RootPhysicalRel(
            root.getCluster(),
            root.getTraitSet().plus(HazelcastConventions.HAZELCAST_PHYSICAL).plus(PhysicalDistributionTrait.SINGLETON),
            convertedInput
        );

        call.transformTo(rootPhysical);
    }
}
