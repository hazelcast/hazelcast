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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

        Mappings.TargetMapping mapping = project.getMapping();

        if (mapping != null) {
            processSimple(call, mapping, scan);
        } else {
            processComplex(call, project, scan);
        }
    }

    /**
     * Process simple case when all project expressions are direct field access. The {@code Project} is eliminated completely.
     *
     * @param mapping Projects mapping.
     * @param scan    Scan.
     */
    private static void processSimple(RelOptRuleCall call, Mappings.TargetMapping mapping, TableScan scan) {
        // Get columns defined in the original TableScan.
        HazelcastTable originalTable = OptUtils.extractHazelcastTable(scan);

        List<Integer> originalProjects = originalTable.getProjects();

        // Remap columns from the Project. The result is the projects that will be pushed down to the new TableScan.
        // E.g. consider the table "t[f0, f1, f2]" and the SQL query "SELECT f2, f0":
        //   Original projects: [0(f0), 1(f1), 2(f2)]
        //   New projects:      [2(f2), 0(f0)]
        List<Integer> newProjects = Mappings.apply((Mapping) mapping, originalProjects);

        // Construct the new TableScan with adjusted columns.
        LogicalTableScan newScan = OptUtils.createLogicalScan(
                scan,
                originalTable.withProject(newProjects)
        );

        call.transformTo(newScan);
    }

    /**
     * Process the complex project with expressions. The {@code Project} operator will remain, but the number and the order of
     * columns returned from the {@code TableScan} is adjusted.
     *
     * @param project Project.
     * @param scan    Scan.
     */
    private void processComplex(RelOptRuleCall call, Project project, TableScan scan) {
        HazelcastTable originalTable = OptUtils.extractHazelcastTable(scan);

        // Map projected field references to real scan fields.
        ProjectFieldVisitor projectFieldVisitor = new ProjectFieldVisitor(originalTable.getProjects());

        for (RexNode projectExp : project.getProjects()) {
            projectExp.accept(projectFieldVisitor);
        }

        // Get new scan fields. These are the only fields that are accessed by the project operator.
        List<Integer> newScanProjects = projectFieldVisitor.createNewScanProjects();

        if (newScanProjects.size() == originalTable.getProjects().size()) {
            // The Project operator already references all the fields from the TableScan. No trimming is possible, so further
            // optimization makes no sense.
            // E.g. "SELECT f3, f2, f1 FROM t" where t=[f1, f2, f3]
            return;
        }

        // Create the new TableScan that do not return unused columns.
        LogicalTableScan newScan = OptUtils.createLogicalScan(
                scan,
                originalTable.withProject(newScanProjects)
        );

        // Create new Project with adjusted column references that point to new TableScan fields.
        ProjectConverter projectConverter = projectFieldVisitor.createProjectConverter();

        List<RexNode> newProjects = new ArrayList<>();

        for (RexNode projectExp : project.getProjects()) {
            RexNode newProjectExp = projectExp.accept(projectConverter);

            newProjects.add(newProjectExp);
        }

        LogicalProject newProject = LogicalProject.create(
                newScan,
                project.getHints(),
                newProjects,
                project.getRowType()
        );

        call.transformTo(newProject);
    }

    /**
     * Visitor which collects fields from project expressions and map them to respective scan fields.
     */
    private static final class ProjectFieldVisitor extends RexVisitorImpl<Void> {
        /**
         * Fields from the original TableScan operator.
         */
        private final List<Integer> originalScanFields;

        /**
         * Map from original scan fields to input expressions that must be remapped during the pushdown.
         */
        private final Map<Integer, List<RexInputRef>> scanFieldIndexToProjectInputs = new LinkedHashMap<>();

        private ProjectFieldVisitor(List<Integer> originalScanFields) {
            super(true);

            this.originalScanFields = originalScanFields;
        }

        @Override
        public Void visitInputRef(RexInputRef projectInput) {
            // Get the scan field referenced by the given column expression (RexInputRef).
            // E.g., for the table t[t1, t2, t3] and the constrained TableScan[table=[t, project[2, 0]]], the input
            // reference [$0] (i.e. t3) is mapped to the scan field [2].
            Integer scanField = originalScanFields.get(projectInput.getIndex());

            assert scanField != null;

            // Track all column expressions from the Project operator that refer to the same TableScan field.
            List<RexInputRef> projectInputs = scanFieldIndexToProjectInputs.computeIfAbsent(scanField, (k) -> new ArrayList<>(1));

            projectInputs.add(projectInput);

            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            for (RexNode operand : call.operands) {
                operand.accept(this);
            }

            return null;
        }

        /**
         * @return The list of {@code TableScan} columns referenced by expressions of the {@code Project} operator.
         */
        private List<Integer> createNewScanProjects() {
            return new ArrayList<>(scanFieldIndexToProjectInputs.keySet());
        }

        /**
         * @return The converter that will adjust column references ({@code RexInputRef}) of the new {@code Project} operator.
         */
        private ProjectConverter createProjectConverter() {
            Map<RexNode, Integer> projectExpToScanFieldMap = new HashMap<>();

            int index = 0;

            for (List<RexInputRef> projectInputs : scanFieldIndexToProjectInputs.values()) {
                for (RexNode projectInput : projectInputs) {
                    projectExpToScanFieldMap.put(projectInput, index);
                }

                index++;
            }

            return new ProjectConverter(projectExpToScanFieldMap);
        }
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
