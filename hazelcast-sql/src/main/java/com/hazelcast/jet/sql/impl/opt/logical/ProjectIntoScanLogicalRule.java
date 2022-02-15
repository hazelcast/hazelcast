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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.OptUtils.inlineExpressions;

/**
 * Logical rule that pushes down column references from a {@link Project} into a {@link TableScan} to allow for constrained
 * scans. See {@link HazelcastTable} for more information about constrained scans.
 * <p>
 * Before:
 * <pre>
 * LogicalProject[projects=[+($2, $0)]]]
 *   LogicalTableScan[table[projects=[0, 1, 2]]]
 * </pre>
 * After:
 * <pre>
 * LogicalTableScan[table[projects=[+($2, $0)]]]
 * </pre>
 */
public final class ProjectIntoScanLogicalRule extends RelOptRule {

    public static final ProjectIntoScanLogicalRule INSTANCE = new ProjectIntoScanLogicalRule();

    private ProjectIntoScanLogicalRule() {
        super(
                operand(Project.class, operand(FullScanLogicalRel.class, none())),
                RelFactories.LOGICAL_BUILDER,
                ProjectIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        FullScanLogicalRel scan = call.rel(1);

        HazelcastTable originalTable = OptUtils.extractHazelcastTable(scan);
        List<RexNode> newProjects = inlineExpressions(originalTable.getProjects(), project.getProjects());

        HazelcastRelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                originalTable.withProject(newProjects, project.getRowType()),
                scan.getCluster().getTypeFactory()
        );

        FullScanLogicalRel rel = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                convertedTable,
                scan.eventTimePolicyProvider(),
                scan.watermarkedColumnIndex()
        );
        call.transformTo(rel);
    }
}
