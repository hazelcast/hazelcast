/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

/**
 * Rule to convert the logical root node to physical root node.
 */
public final class RootPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new RootPhysicalRule();

    private RootPhysicalRule() {
        super(
            OptUtils.parentChild(RootLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            RootPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RootLogicalRel logicalRoot = call.rel(0);
        RelNode input = call.rel(1);

        RelNode convertedInput = OptUtils.toPhysicalInput(input, OptUtils.getDistributionDef(input).getTraitRoot());

        RootPhysicalRel transformedRoot = new RootPhysicalRel(
            logicalRoot.getCluster(),
            convertedInput.getTraitSet(),
            convertedInput
        );

        call.transformTo(transformedRoot);
    }
}
