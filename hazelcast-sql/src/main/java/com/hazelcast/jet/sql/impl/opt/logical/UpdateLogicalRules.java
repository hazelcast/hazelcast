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
import com.hazelcast.sql.impl.schema.TableField;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

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
                    RelOptCluster cluster = scan.getCluster();

                    return new FullScanLogicalRel(
                            scan.getCluster(),
                            OptUtils.toLogicalConvention(scan.getTraitSet()),
                            OptUtils.createRelTable(
                                    relTable.getDelegate().getQualifiedName(),
                                    hazelcastTable.withProject(
                                            keyProjects(cluster, hazelcastTable.getTarget()), project.getRowType()),
                                    scan.getCluster().getTypeFactory()),
                            null,
                            -1);
                }

                private List<RexNode> keyProjects(RelOptCluster cluster, Table table) {
                    List<String> primaryKey = SqlConnectorUtil.getJetSqlConnector(table).getPrimaryKey(table);
                    List<RexNode> projects = new ArrayList<>();
                    int idx = 0;
                    for (TableField field : table.getFields()) {
                        if (primaryKey.contains(field.getName())) {
                            projects.add(new RexInputRef(idx++, OptUtils.convert(field, cluster.getTypeFactory())));
                        }
                    }
                    return projects;
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

    private UpdateLogicalRules() {
    }
}
