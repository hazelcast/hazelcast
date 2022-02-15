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

import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.opt.OptUtils.inlineExpression;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.inlineExpressions;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.operandJ;

// no support for UPDATE FROM SELECT case (which is not yet implemented)
// once joins are there we need to create complementary rule
final class UpdateLogicalRules {

    @SuppressWarnings("checkstyle:anoninnerlength")
    static final RelOptRule INSTANCE =
            new RelOptRule(
                    operandJ(
                            LogicalTableModify.class, null, TableModify::isUpdate,
                            operand(
                                    LogicalProject.class,
                                    operand(LogicalTableScan.class, none())
                            )
                    ),
                    UpdateLogicalRules.class.getSimpleName()
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    LogicalTableModify update = call.rel(0);
                    LogicalProject project = call.rel(1);
                    LogicalTableScan scan = call.rel(2);

                    UpdateLogicalRel rel = new UpdateLogicalRel(
                            update.getCluster(),
                            OptUtils.toLogicalConvention(update.getTraitSet()),
                            update.getTable(),
                            update.getCatalogReader(),
                            rewriteScan(scan, project),
                            update.getUpdateColumnList(),
                            update.getSourceExpressionList(),
                            update.isFlattened()
                    );
                    call.transformTo(rel);
                }

                // rewrites existing project to just primary keys project
                private RelNode rewriteScan(LogicalTableScan scan, LogicalProject project) {
                    HazelcastRelOptTable relTable = (HazelcastRelOptTable) scan.getTable();
                    HazelcastTable hazelcastTable = relTable.unwrap(HazelcastTable.class);

                    List<RexNode> newProjects = inlineExpressions(hazelcastTable.getProjects(), project.getProjects());
                    List<RexNode> keyProjects = keyProjects(hazelcastTable.getTarget(), newProjects);
                    List<RelDataTypeField> fields = new ArrayList<>();
                    int idx = 0;
                    for (RexNode keyProject : keyProjects) {
                        RexInputRef inputRef = (RexInputRef) keyProject;
                        RelDataTypeField fieldType = new RelDataTypeFieldImpl(
                                inputRef.getName(), idx, inputRef.getType()
                        );
                        fields.add(idx++, fieldType);
                    }
                    RelDataType relDataType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);

                    HazelcastRelOptTable convertedTable = OptUtils.createRelTable(
                            relTable,
                            hazelcastTable.withProject(keyProjects, relDataType),
                            scan.getCluster().getTypeFactory()
                    );

                    FullScanLogicalRel rel = new FullScanLogicalRel(
                            scan.getCluster(),
                            OptUtils.toLogicalConvention(scan.getTraitSet()),
                            convertedTable,
                            null,
                            -1
                    );
                    return rel;
                }
            };


    @SuppressWarnings("checkstyle:AnonInnerLength")
    static final RelOptRule FILTER_INSTANCE =
            new RelOptRule(
                    operandJ(
                            LogicalTableModify.class, null, TableModify::isUpdate,
                            operand(
                                    LogicalProject.class,
                                    operand(
                                            LogicalFilter.class,
                                            operand(LogicalTableScan.class, none()))
                            )
                    ),
                    UpdateLogicalRules.class.getSimpleName() + "_filter"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    LogicalTableModify update = call.rel(0);
                    LogicalProject project = call.rel(1);
                    LogicalFilter filter = call.rel(2);
                    LogicalTableScan scan = call.rel(3);

                    UpdateLogicalRel rel = new UpdateLogicalRel(
                            update.getCluster(),
                            OptUtils.toLogicalConvention(update.getTraitSet()),
                            update.getTable(),
                            update.getCatalogReader(),
                            rewriteScan(scan, project, filter),
                            update.getUpdateColumnList(),
                            update.getSourceExpressionList(),
                            update.isFlattened()
                    );
                    call.transformTo(rel);
                }

                // rewrites existing project to just primary keys project
                private RelNode rewriteScan(LogicalTableScan scan, LogicalProject project, LogicalFilter filter) {
                    HazelcastRelOptTable relTable = (HazelcastRelOptTable) scan.getTable();
                    HazelcastTable hazelcastTable = relTable.unwrap(HazelcastTable.class);

                    List<RexNode> newProjects = inlineExpressions(hazelcastTable.getProjects(), project.getProjects());
                    List<RexNode> keyProjects = keyProjects(hazelcastTable.getTarget(), newProjects);
                    List<RelDataTypeField> fields = new ArrayList<>();
                    int idx = 0;
                    for (RexNode keyProject : keyProjects) {
                        RexInputRef inputRef = (RexInputRef) keyProject;
                        RelDataTypeField fieldType = new RelDataTypeFieldImpl(
                                inputRef.getName(), idx, inputRef.getType()
                        );
                        fields.add(idx++, fieldType);
                    }
                    RelDataType relDataType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
                    RexNode newCondition = inlineExpression(newProjects, filter.getCondition());

                    HazelcastRelOptTable convertedTable = OptUtils.createRelTable(
                            relTable,
                            hazelcastTable.withProject(keyProjects, relDataType).withFilter(newCondition),
                            scan.getCluster().getTypeFactory()
                    );

                    FullScanLogicalRel rel = new FullScanLogicalRel(
                            scan.getCluster(),
                            OptUtils.toLogicalConvention(scan.getTraitSet()),
                            convertedTable,
                            null,
                            -1
                    );
                    return rel;
                }
            };

    // no-updates case, i.e. '... WHERE __key = 1 AND __key = 2'
    // could/should be optimized to no-op
    static final RelOptRule NOOP_INSTANCE =
            new RelOptRule(
                    operandJ(
                            LogicalTableModify.class, null, TableModify::isUpdate,
                            operand(LogicalValues.class, none())
                    ),
                    UpdateLogicalRules.class.getSimpleName() + "(NOOP)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    LogicalTableModify update = call.rel(0);
                    LogicalValues values = call.rel(1);

                    UpdateLogicalRel rel = new UpdateLogicalRel(
                            update.getCluster(),
                            OptUtils.toLogicalConvention(update.getTraitSet()),
                            update.getTable(),
                            update.getCatalogReader(),
                            OptUtils.toLogicalInput(values),
                            update.getUpdateColumnList(),
                            update.getSourceExpressionList(),
                            update.isFlattened()
                    );
                    call.transformTo(rel);
                }
            };

    private static List<RexNode> keyProjects(Table table, List<RexNode> projects) {
        List<RexNode> keyProjects = new ArrayList<>();
        Set<String> primaryKeys = new HashSet<>(SqlConnectorUtil.getJetSqlConnector(table).getPrimaryKey(table));
        for (RexNode project : projects) {
            // Only RexInputRef may be key project, if even exists.
            if (project instanceof RexInputRef) {
                RexInputRef rexInputRef = (RexInputRef) project;
                String inputRefName = table.getField(rexInputRef.getIndex()).getName();
                if (primaryKeys.contains(inputRefName)) {
                    keyProjects.add(rexInputRef);
                }
            }
        }
        return keyProjects;
    }

    private UpdateLogicalRules() {
    }
}
