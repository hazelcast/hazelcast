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
import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastTableScanRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.HazelcastDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastRootPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastTableScanPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class HazelcastRootPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new HazelcastRootPhysicalRule();

    private HazelcastRootPhysicalRule() {
        super(
            RelOptRule.operand(HazelcastRootRel.class, RelOptRule.some(RelOptRule.operand(RelNode.class, RelOptRule.any()))),
            "HazelcastRootPhysicalRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        HazelcastRootRel root = call.rel(0);
        RelNode input = call.rel(1);

        // TODO: Proper partitioning.
        RelTraitSet traits = input.getTraitSet()
            .plus(HazelcastPhysicalRel.HAZELCAST_PHYSICAL)
            .plus(HazelcastDistributionTrait.SINGLETON);

        final RelNode convertedInput = convert(input, traits.simplify());

        // TODO: Add proper partitioning.
        HazelcastRootPhysicalRel rootPhysical = new HazelcastRootPhysicalRel(
            root.getCluster(),
            root.getTraitSet().plus(HazelcastPhysicalRel.HAZELCAST_PHYSICAL).plus(HazelcastDistributionTrait.SINGLETON),
            convertedInput
        );

        call.transformTo(rootPhysical);
    }
}
