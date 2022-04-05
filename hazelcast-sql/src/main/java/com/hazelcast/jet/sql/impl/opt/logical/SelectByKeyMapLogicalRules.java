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
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operandJ;

/**
 * A collection of planner rules that match single key, constant expression,
 * {@link PartitionedMapTable} SELECT.
 * <p>For example,</p>
 * <blockquote><code>SELECT * FROM map WHERE __key = 1</code></blockquote>
 * or
 * <blockquote><code>SELECT this + 1 FROM map WHERE __key = 1</code></blockquote>
 * <p>
 * Such SELECT is translated to optimized, direct-key {@code IMap} operation
 * which does not involve starting any job.
 */
final class SelectByKeyMapLogicalRules {

    static final RelOptRule INSTANCE = new RelOptRule(
            operandJ(
                    TableScan.class,
                    null,
                    scan -> !OptUtils.requiresJob(scan) && OptUtils.hasTableType(scan, PartitionedMapTable.class),
                    none()
            ),
            SelectByKeyMapLogicalRules.class.getSimpleName()
    ) {
        @Override
        public void onMatch(RelOptRuleCall call) {
            TableScan scan = call.rel(0);

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
                        OptUtils.extractHazelcastTable(scan).getProjects()
                );
                call.transformTo(rel);
            }
        }
    };

    private SelectByKeyMapLogicalRules() {
    }
}
