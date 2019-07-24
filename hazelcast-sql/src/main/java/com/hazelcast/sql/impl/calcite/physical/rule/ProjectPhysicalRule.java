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
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.ProjectLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.ProjectPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * This rule converts logical projection into physical projection. Physical projection inherits distribution of the
 * underlying operator.
 */
public class ProjectPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new ProjectPhysicalRule();

    private ProjectPhysicalRule() {
        super(
            RuleUtils.parentChild(ProjectLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            ProjectPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        ProjectLogicalRel project = call.rel(0);
        RelNode input = project.getInput();

        RelTraitSet traits = input.getTraitSet()
            .plus(HazelcastConventions.PHYSICAL)
            .plus(PhysicalDistributionTrait.ANY);

        RelNode physicalInput = convert(input, traits.simplify());

        ProjectPhysicalRel physicalProject = new ProjectPhysicalRel(
            project.getCluster(),
            project.getTraitSet().plus(HazelcastConventions.PHYSICAL).plus(PhysicalDistributionTrait.ANY),
            physicalInput,
            project.getProjects(),
            project.getRowType()
        );

        call.transformTo(physicalProject);
    }
}
