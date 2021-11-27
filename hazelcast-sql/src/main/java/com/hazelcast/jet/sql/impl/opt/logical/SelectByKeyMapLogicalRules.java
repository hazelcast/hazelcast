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
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.List;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.operandJ;

/**
 * A collection of planner rules that match single key, constant expression,
 * {@link PartitionedMapTable} SELECT.
 * <p>For example,</p>
 * <blockquote><code>SELECT * FROM map WHERE __key = 1</code></blockquote>
 * or
 * <blockquote><code>SELECT this + 1 FROM map WHERE __key = 1</code></blockquote>
 * <p>
 * Such SELECT is translated to optimized, direct key {@code IMap} operation
 * which does not involve starting any job.
 */
final class SelectByKeyMapLogicalRules {

    static final RelOptRule INSTANCE = new RelOptRule(
            operandJ(
                    LogicalTableScan.class,
                    null,
                    scan -> !OptUtils.requiresJob(scan) && OptUtils.hasTableType(scan, PartitionedMapTable.class),
                    none()
            ),
            SelectByKeyMapLogicalRules.class.getSimpleName()
    ) {
        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalTableScan scan = call.rel(0);

            RelOptTable table = scan.getTable();
            RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
            RexNode keyCondition = OptUtils.extractKeyConstantExpression(table, rexBuilder);
            if (keyCondition != null) {
                SelectByKeyMapLogicalRel rel = new SelectByKeyMapLogicalRel(
                        scan.getCluster(),
                        OptUtils.toLogicalConvention(scan.getTraitSet()),
                        scan.getRowType(),
                        table,
                        keyCondition,
                        pushProjectIntoTable(rexBuilder.identityProjects(scan.getRowType()), table)
                );
                call.transformTo(rel);
            }
        }
    };

    static final RelOptRule PROJECT_INSTANCE = new RelOptRule(
            operand(
                    LogicalProject.class,
                    operandJ(
                            LogicalTableScan.class,
                            null,
                            scan -> !OptUtils.requiresJob(scan) && OptUtils.hasTableType(scan, PartitionedMapTable.class),
                            none()
                    )
            ),
            SelectByKeyMapLogicalRules.class.getSimpleName() + "(Project)"
    ) {
        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);
            LogicalTableScan scan = call.rel(1);

            RelOptTable table = scan.getTable();
            RexNode keyCondition = OptUtils.extractKeyConstantExpression(table, project.getCluster().getRexBuilder());
            if (keyCondition != null) {
                SelectByKeyMapLogicalRel rel = new SelectByKeyMapLogicalRel(
                        scan.getCluster(),
                        OptUtils.toLogicalConvention(scan.getTraitSet()),
                        project.getRowType(),
                        table,
                        keyCondition,
                        pushProjectIntoTable(project.getProjects(), table)
                );
                call.transformTo(rel);
            }
        }
    };

    private SelectByKeyMapLogicalRules() {
    }

    /**
     * Inline the projection from {@code table} into the projection given in {@code projects}.
     *
     * For example, the table's projection is {@code [4, 5]} (meaning
     * the table outputs fifth and sixth fields from the underlying
     * table), and {@code projects} is {@code [$1 + 5]} (meaning 5 added
     * to the second field of the input). In this case the output will
     * be {@code [$5 + 5]}.
     */
    private static List<RexNode> pushProjectIntoTable(List<RexNode> projects, RelOptTable input) {
        HazelcastTable hzTable = input.unwrap(HazelcastTable.class);
        assert hzTable != null;
        List<RelDataTypeField> fieldList = input.getRowType().getFieldList();

        RexShuttle shuttle =  new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int index = ref.getIndex();
                return new RexInputRef(hzTable.getProjects().get(index), fieldList.get(index).getType());
            }
        };

        return shuttle.apply(projects);
    }
}
