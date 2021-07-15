/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.logical.ValuesLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Rule to convert the logical values node to physical values node.
 */
public final class ValuesPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new ValuesPhysicalRule();

    private ValuesPhysicalRule() {
        super(
                OptUtils.single(ValuesLogicalRel.class, HazelcastConventions.LOGICAL),
                ValuesPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        ValuesLogicalRel logicalValues = call.rel(0);

        DistributionTrait distribution = logicalValues.getHazelcastCluster().getDistributionTraitDef().getTraitReplicated();

        ValuesPhysicalRel transformedValues = new ValuesPhysicalRel(
                logicalValues.getCluster(),
                logicalValues.getRowType(),
                logicalValues.getTuples(),
                OptUtils.toPhysicalConvention(logicalValues.getTraitSet(), distribution)
        );

        call.transformTo(transformedValues);
    }
}
