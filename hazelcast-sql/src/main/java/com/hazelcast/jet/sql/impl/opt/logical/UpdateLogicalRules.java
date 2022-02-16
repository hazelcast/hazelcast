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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.operandJ;

// no support for UPDATE FROM SELECT case (which is not yet implemented)
// once joins are there we need to create complementary rule
final class UpdateLogicalRules {

    @SuppressWarnings("checkstyle:anoninnerlength")
    static final RelOptRule SCAN_INSTANCE =
            new RelOptRule(
                    operandJ(
                            TableModifyLogicalRel.class, LOGICAL, TableModify::isUpdate,
                            operand(FullScanLogicalRel.class, none())
                    ),
                    UpdateLogicalRules.class.getSimpleName()
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    TableModifyLogicalRel update = call.rel(0);
                    FullScanLogicalRel scan = call.rel(1);

                    UpdateLogicalRel rel = new UpdateLogicalRel(
                            update.getCluster(),
                            OptUtils.toLogicalConvention(update.getTraitSet()),
                            update.getTable(),
                            update.getCatalogReader(),
                            rewriteScan(scan),
                            update.getUpdateColumnList(),
                            update.getSourceExpressionList(),
                            update.isFlattened()
                    );
                    call.transformTo(rel);
                }

                // rewrites existing project to just primary keys project
                private RelNode rewriteScan(FullScanLogicalRel scan) {
                    HazelcastRelOptTable relTable = (HazelcastRelOptTable) scan.getTable();
                    HazelcastTable hzTable = relTable.unwrap(HazelcastTable.class);

                    List<RexNode> keyProjects = OptUtils.keyProjects(hzTable.getTarget(), hzTable.getProjects());

                    HazelcastRelOptTable convertedTable = OptUtils.createRelTable(
                            relTable,
                            hzTable.withProject(keyProjects, OptUtils.computeRelDataType(keyProjects)),
                            scan.getCluster().getTypeFactory()
                    );

                    return new FullScanLogicalRel(
                            scan.getCluster(),
                            OptUtils.toLogicalConvention(scan.getTraitSet()),
                            convertedTable,
                            null,
                            -1
                    );
                }
            };

    // Calcite replaces the table scan with empty VALUES when the WHERE clause
    // is always false
    // i.e. '... WHERE __key = 1 AND __key = 2'
    // could/should be optimized to no-op
    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule VALUES_INSTANCE =
            new RelOptRule(
                    operandJ(
                            TableModifyLogicalRel.class, null, TableModify::isUpdate,
                            operand(ValuesLogicalRel.class, none())
                    ),
                    UpdateLogicalRules.class.getSimpleName() + "(NOOP)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    TableModifyLogicalRel update = call.rel(0);
                    ValuesLogicalRel values = call.rel(1);

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
