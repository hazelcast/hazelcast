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

package com.hazelcast.sql.impl.calcite.logical.rule;

import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.ProjectLogicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ProjectIntoScanLogicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new ProjectIntoScanLogicalRule();

    private ProjectIntoScanLogicalRule() {
        super(
            RuleUtils.parentChild(LogicalProject.class, LogicalTableScan.class, Convention.NONE),
            RelFactories.LOGICAL_BUILDER,
            ProjectIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);

        if (scan.getRowType().getFieldList().isEmpty()) {
            return;
        }

        // Map projected field references to real scan fields.
        ProjectFieldVisitor projectFieldVisitor = new ProjectFieldVisitor(scan.getRowType().getFieldList());

        for (RexNode projectExp : project.getProjects()) {
            projectExp.accept(projectFieldVisitor);
        }

        projectFieldVisitor.done(scan.getCluster().getTypeFactory());

        // Construct new scan operator with the given type.
        MapScanLogicalRel newScan = new MapScanLogicalRel(
            scan.getCluster(),
            RuleUtils.toLogicalConvention(scan.getTraitSet()),
            scan.getTable(),
            projectFieldVisitor.getNewScanDataType()
        );

        // Create new project nodes with references to scan fields.
        ProjectConverter projectConverter = projectFieldVisitor.getNewProjectConverter();

        List<RexNode> newProjects = new ArrayList<>();

        for (RexNode projectExp : project.getProjects()) {
            RexNode newProjectExp = projectExp.accept(projectConverter);

            newProjects.add(newProjectExp);
        }

        ProjectLogicalRel newProject = new ProjectLogicalRel(
            project.getCluster(),
            RuleUtils.toLogicalConvention(project.getTraitSet()),
            newScan,
            newProjects,
            project.getRowType()
        );

        // If new project is trivial, i.e. it contains only references to scan fields, then it can be eliminated.
        if (ProjectRemoveRule.isTrivial(newProject)) {
            call.transformTo(newScan);
        } else {
            call.transformTo(newProject);
        }
    }

    /**
     * Visitor which collects fields from project expressions and map them to respective scna fields.
     */
    private static final class ProjectFieldVisitor extends RexVisitorImpl<Void> {
        /** Scan fields. */
        private final List<RelDataTypeField> scanFields;

        /** Mapped project fields. */
        private final Map<String, MappedScanField> mappedScanFields = new LinkedHashMap<>();

        /** Data type for the new scan operator. */
        private RelDataType newScanDataType;

        /** Converter for expressions of the new project operator. */
        private ProjectConverter newProjectConverter;

        private ProjectFieldVisitor(List<RelDataTypeField> scanFields) {
            super(true);

            this.scanFields = scanFields;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            RelDataTypeField scanField = scanFields.get(inputRef.getIndex());

            MappedScanField mappedScanField =
                mappedScanFields.computeIfAbsent(scanField.getName(), (k) -> new MappedScanField(scanField));

            mappedScanField.addNode(inputRef);

            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            // TODO: Support "star" and "item".

            for (RexNode operand : call.operands) {
                operand.accept(this);
            }

            return null;
        }

        private void done(RelDataTypeFactory typeFactory) {
            List<String> fieldNames = new ArrayList<>(1);
            List<RelDataType> types = new ArrayList<>(1);

            Map<RexNode, Integer> projectExpToScanFieldMap = new HashMap<>();

            int index = 0;

            for (MappedScanField mappedField : mappedScanFields.values()) {
                fieldNames.add(mappedField.getScanField().getName());
                types.add(mappedField.getScanField().getType());

                for (RexNode projectNode : mappedField.getProjectNodes()) {
                    projectExpToScanFieldMap.put(projectNode, index);
                }

                index++;
            }

            newProjectConverter = new ProjectConverter(projectExpToScanFieldMap);
            newScanDataType = typeFactory.createStructType(types, fieldNames);
        }

        private RelDataType getNewScanDataType() {
            return newScanDataType;
        }

        private ProjectConverter getNewProjectConverter() {
            return newProjectConverter;
        }
    }

    /**
     * Visitor which converts old project expressions (before pushdown) to new project expressions (after pushdown).
     */
    public static final class ProjectConverter extends RexShuttle {
        /** Map from old project expression to relevant field in the new scan operator. */
        private final Map<RexNode, Integer> projectExpToScanFieldMap;

        private ProjectConverter(Map<RexNode, Integer> projectExpToScanFieldMap) {
            this.projectExpToScanFieldMap = projectExpToScanFieldMap;
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            Integer index = projectExpToScanFieldMap.get(call);

            if (index != null) {
                return new RexInputRef(index, call.getType());
            }

            return super.visitCall(call);
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            Integer index = projectExpToScanFieldMap.get(inputRef);

            if (index != null) {
                return new RexInputRef(index, inputRef.getType());
            }

            return super.visitInputRef(inputRef);
        }
    }

    /**
     * Scan field mapped to project REX nodes.
     */
    public static class MappedScanField {
        private final RelDataTypeField scanField;
        private final List<RexNode> projectNodes = new ArrayList<>(1);

        public MappedScanField(RelDataTypeField scanField) {
            this.scanField = scanField;
        }

        public void addNode(RexNode projectNode) {
            projectNodes.add(projectNode);
        }

        public RelDataTypeField getScanField() {
            return scanField;
        }

        public List<RexNode> getProjectNodes() {
            return projectNodes;
        }
    }
}
