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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.ProjectLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

final class ProjectPhysicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new ProjectPhysicalRule();

    private ProjectPhysicalRule() {
        super(
                operand(ProjectLogicalRel.class, LOGICAL, some(operand(RelNode.class, any()))),
                ProjectPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        ProjectLogicalRel logicalProject = call.rel(0);
        RelNode input = logicalProject.getInput();

        RelNode convertedInput = OptUtils.toPhysicalInput(input);
        Collection<RelNode> transformedInputs = OptUtils.extractPhysicalRelsFromSubset(convertedInput);
        for (RelNode transformedInput : transformedInputs) {
            ProjectPhysicalRel rel = new ProjectPhysicalRel(
                    logicalProject.getCluster(),
                    transformedInput.getTraitSet(),
                    transformedInput,
                    logicalProject.getProjects(),
                    logicalProject.getRowType()
            );
            call.transformTo(rel);
        }
    }
}
