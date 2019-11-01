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

package com.hazelcast.sql.impl.calcite.rule.logical;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.rel.logical.ProjectLogicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;

public final class ProjectLogicalRule extends ConverterRule {
    public static final RelOptRule INSTANCE = new ProjectLogicalRule();

    private ProjectLogicalRule() {
        super(
            LogicalProject.class,
            Convention.NONE,
            HazelcastConventions.LOGICAL,
            ProjectLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        RelNode input = project.getInput();

        ProjectLogicalRel newProject = new ProjectLogicalRel(
            project.getCluster(),
            RuleUtils.toLogicalConvention(project.getTraitSet()),
            RuleUtils.toLogicalInput(input),
            project.getProjects(),
            project.getRowType()
        );

        call.transformTo(newProject);
    }

    @Override
    public RelNode convert(RelNode rel) {
        Project project = (Project) rel;
        RelNode input = project.getInput();

        return new ProjectLogicalRel(
            project.getCluster(),
            RuleUtils.toLogicalConvention(project.getTraitSet()),
            RuleUtils.toLogicalInput(input),
            project.getProjects(),
            project.getRowType()
        );
    }
}
