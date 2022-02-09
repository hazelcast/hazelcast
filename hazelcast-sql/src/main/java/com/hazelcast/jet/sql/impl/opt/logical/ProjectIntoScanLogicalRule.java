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
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

import java.util.List;
import java.util.Map;

/**
 * Logical rule that pushes down column references from a {@link Project} into a {@link TableScan} to allow for constrained
 * scans. See {@link HazelcastTable} for more information about constrained scans.
 * <p>
 * <b>Case 1: </b>projects that have only column expressions are eliminated completely, unused columns returned from the
 * {@code TableScan} are trimmed.
 * <p>
 * Before:
 * <pre>
 * LogicalProject[projects=[$2, $1]]]
 *   LogicalTableScan[table[projects=[0, 1, 2]]]
 * </pre>
 * After:
 * <pre>
 * LogicalTableScan[table[projects=[2, 1]]]
 * </pre>
 * <b>Case 2: </b>projects with non-column expressions trim the unused columns only.
 * <p>
 * Before:
 * <pre>
 * LogicalProject[projects=[+$2, $0]]]
 *   LogicalTableScan[table[projects=[0, 1, 2]]]
 * </pre>
 * After:
 * <pre>
 * LogicalProject[projects=[+$0, $1]]]
 *   LogicalTableScan[table[projects=[2, 0]]]
 * </pre>
 */
public final class ProjectIntoScanLogicalRule extends RelOptRule {

    public static final ProjectIntoScanLogicalRule INSTANCE = new ProjectIntoScanLogicalRule();

    private ProjectIntoScanLogicalRule() {
        super(
                operand(LogicalProject.class, operand(LogicalTableScan.class, none())),
                RelFactories.LOGICAL_BUILDER,
                ProjectIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);

        processProjectPushdown(call, project, scan);
    }

    /**
     * Process {@code Project} with complete rel elimination.
     *
     * @param project Project rel.
     * @param scan    Scan rel.
     */
    private static void processProjectPushdown(RelOptRuleCall call, Project project, TableScan scan) {
        // Get columns defined in the original TableScan.
        HazelcastTable originalTable = OptUtils.extractHazelcastTable(scan);
        TargetMapping mapping = project.getMapping();

        List<RexNode> newProjects;

        if (mapping != null) {
            // Remap columns from the Project. The result is the projects that will be pushed down to the new TableScan.
            // E.g. consider the table "t[f0, f1, f2]" and the SQL query "SELECT f2, f0":
            //   Original projects: [0(f0), 1(f1), 2(f2)]
            //   New projects:      [2(f2), 0(f0)]
            newProjects = Mappings.apply((Mapping) mapping, originalTable.getProjects());
        } else {
            // TODO!
            newProjects = project.getProjects();
        }

        // Construct the new TableScan with adjusted columns.
        LogicalTableScan newScan = OptUtils.createLogicalScan(
                scan,
                originalTable.withProject(newProjects, project.getRowType())
        );

        call.transformTo(newScan);
    }


    /**
     * Visitor which converts old column expressions (before pushdown) to new column expressions (after pushdown).
     */
    public static final class ProjectConverter extends RexShuttle {
        /**
         * Map from old project expression to relevant field in the new scan operator.
         */
        private final Map<RexNode, Integer> projectExpToScanFieldMap;

        private ProjectConverter(Map<RexNode, Integer> projectExpToScanFieldMap) {
            this.projectExpToScanFieldMap = projectExpToScanFieldMap;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            Integer index = projectExpToScanFieldMap.get(inputRef);

            assert index != null;

            return new RexInputRef(index, inputRef.getType());
        }
    }
}
