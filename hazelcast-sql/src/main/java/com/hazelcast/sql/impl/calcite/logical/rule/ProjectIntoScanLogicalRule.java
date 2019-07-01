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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.sql.impl.calcite.logical.rel.ProjectLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.LogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.MapScanLogicalRel;
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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// TODO: Remove Drill copy-paste.
public class ProjectIntoScanLogicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new ProjectIntoScanLogicalRule();

    private ProjectIntoScanLogicalRule() {
        super(
            // TODO: Set NONE convention.
            RelOptRule.operand(LogicalProject.class, RelOptRule.some(RelOptRule.operand(LogicalTableScan.class, RelOptRule.any()))),
            RelFactories.LOGICAL_BUILDER,
            "ProjectIntoScanLogicalRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);

        // TODO: Why?
        if (scan.getRowType().getFieldList().isEmpty())
            return;

        ProjectPushInfo projectPushInfo = getFieldsInformation(scan.getRowType(), project.getProjects());

        MapScanLogicalRel newScan = createScan(scan, projectPushInfo);

        List<RexNode> newProjects = new ArrayList<>();
        for (RexNode n : project.getChildExps()) {
            newProjects.add(n.accept(projectPushInfo.getInputReWriter()));
        }

        ProjectLogicalRel newProject = createProject(project, newScan, newProjects);

        if (ProjectRemoveRule.isTrivial(newProject))
            call.transformTo(newScan);
        else
            call.transformTo(newProject);
    }

    public static ProjectPushInfo getFieldsInformation(RelDataType rowType, List<RexNode> projects) {
        ProjectFieldsVisitor fieldsVisitor = new ProjectFieldsVisitor(rowType);
        for (RexNode exp : projects) {
            exp.accept(fieldsVisitor);
        }

        return fieldsVisitor.getInfo();
    }

    public static class ProjectPushInfo {
        private final FieldsReWriter reWriter;
        private final List<String> fieldNames;
        private final List<RelDataType> types;

        public ProjectPushInfo(Map<String, DesiredField> desiredFields) {
            this.fieldNames = new ArrayList<>();
            this.types = new ArrayList<>();

            Map<RexNode, Integer> mapper = new HashMap<>();

            int index = 0;
            for (Map.Entry<String, DesiredField> entry : desiredFields.entrySet()) {
                fieldNames.add(entry.getKey());
                DesiredField desiredField = entry.getValue();
                types.add(desiredField.getType());
                for (RexNode node : desiredField.getNodes()) {
                    mapper.put(node, index);
                }
                index++;
            }
            this.reWriter = new FieldsReWriter(mapper);
        }

        public FieldsReWriter getInputReWriter() {
            return reWriter;
        }

        /**
         * Creates new row type based on stores types and field names.
         *
         * @param factory factory for data type descriptors.
         * @return new row type
         */
        public RelDataType createNewRowType(RelDataTypeFactory factory) {
            return factory.createStructType(types, fieldNames);
        }
    }

    public static class DesiredField {
        private final String name;
        private final RelDataType type;
        private final List<RexNode> nodes = new ArrayList<>();

        public DesiredField(String name, RelDataType type, RexNode node) {
            this.name = name;
            this.type = type;
            addNode(node);
        }

        public void addNode(RexNode originalNode) {
            nodes.add(originalNode);
        }

        public String getName() {
            return name;
        }

        public RelDataType getType() {
            return type;
        }

        public List<RexNode> getNodes() {
            return nodes;
        }
    }

    public static class FieldsReWriter extends RexShuttle {

        private final Map<RexNode, Integer> mapper;

        public FieldsReWriter(Map<RexNode, Integer> mapper) {
            this.mapper = mapper;
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            Integer index = mapper.get(call);
            if (index != null) {
                return new RexInputRef(index, call.getType());
            }
            return super.visitCall(call);
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            Integer index = mapper.get(inputRef);
            if (index != null) {
                return new RexInputRef(index, inputRef.getType());
            }
            return super.visitInputRef(inputRef);
        }

    }

    private static class ProjectFieldsVisitor extends RexVisitorImpl<String> {
        private final List<String> fieldNames;
        private final List<RelDataTypeField> fields;

        private final Map<String, DesiredField> desiredFields = new LinkedHashMap<>();

        ProjectFieldsVisitor(RelDataType rowType) {
            super(true);
            this.fieldNames = rowType.getFieldNames();
            this.fields = rowType.getFieldList();
        }

        ProjectPushInfo getInfo() {
            return new ProjectPushInfo(ImmutableMap.copyOf(desiredFields));
        }

        @Override
        public String visitInputRef(RexInputRef inputRef) {
            int index = inputRef.getIndex();
            String name = fieldNames.get(index);
            RelDataTypeField field = fields.get(index);
            addDesiredField(name, field.getType(), inputRef);
            return name;
        }

        @Override
        public String visitCall(RexCall call) {
            String itemStarFieldName = getFieldNameFromItemStarField(call, fieldNames);
            if (itemStarFieldName != null) {
                addDesiredField(itemStarFieldName, call.getType(), call);
                return itemStarFieldName;
            }

            if (SqlStdOperatorTable.ITEM.equals(call.getOperator())) {
                String mapOrArray = call.operands.get(0).accept(this);
                if (mapOrArray != null)
                    return mapOrArray;
            }
            else {
                for (RexNode operand : call.operands) {
                    operand.accept(this);
                }
            }
            return null;
        }

        private void addDesiredField(String name, RelDataType type, RexNode originalNode) {
            DesiredField desiredField = desiredFields.get(name);
            if (desiredField == null) {
                desiredFields.put(name, new DesiredField(name, type, originalNode));
            } else {
                desiredField.addNode(originalNode);
            }
        }
    }

    public static String getFieldNameFromItemStarField(RexCall rexCall, List<String> fieldNames) {
        if (!SqlStdOperatorTable.ITEM.equals(rexCall.getOperator())) {
            return null;
        }

        if (rexCall.getOperands().size() != 2) {
            return null;
        }

        if (!(rexCall.getOperands().get(0) instanceof RexInputRef && rexCall.getOperands().get(1) instanceof RexLiteral)) {
            return null;
        }

        // get parent field reference from the first operand (ITEM($0, 'col_name' -> $0)
        // and check if it corresponds to the dynamic star
        RexInputRef rexInputRef = (RexInputRef) rexCall.getOperands().get(0);
        String parentFieldName = fieldNames.get(rexInputRef.getIndex());

        // get field name from the second operand (ITEM($0, 'col_name') -> col_name)
        RexLiteral rexLiteral = (RexLiteral) rexCall.getOperands().get(1);
        if (SqlTypeName.CHAR.equals(rexLiteral.getType().getSqlTypeName())) {
            return RexLiteral.stringValue(rexLiteral);
        }

        return null;
    }

    /**
     * Creates new {@code DrillScanRelBase} instance with row type and fields list
     * obtained from specified {@code ProjectPushInfo projectPushInfo}
     * using specified {@code TableScan scan} as prototype.
     *
     * @param scan            the prototype of resulting scan
     * @param projectPushInfo the source of row type and fields list
     * @return new scan instance
     */
    protected MapScanLogicalRel createScan(TableScan scan, ProjectPushInfo projectPushInfo) {
        return new MapScanLogicalRel(scan.getCluster(),
            scan.getTraitSet().plus(LogicalRel.LOGICAL),
            scan.getTable(),
            projectPushInfo.createNewRowType(scan.getCluster().getTypeFactory())
        );
    }

    protected ProjectLogicalRel createProject(Project project, TableScan newScan, List<RexNode> newProjects) {
        return new ProjectLogicalRel(project.getCluster(),
            project.getTraitSet().plus(LogicalRel.LOGICAL),
            newScan,
            newProjects,
            project.getRowType()
        );
    }
}
