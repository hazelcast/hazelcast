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
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

/**
 * Planner rule that matches single key, constant expression,
 * {@link PartitionedMapTable} DELETE.
 * <p>For example,</p>
 * <blockquote><code>DELETE FROM map WHERE __key = 1</code></blockquote>
 * <p>
 * Such DELETE is translated to optimized, direct key {@code IMap} operation
 * which does not involve starting any job.
 */
public final class DeleteByKeyMapLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new DeleteByKeyMapLogicalRule();

    private DeleteByKeyMapLogicalRule() {
        super(
                operandJ(
                        TableModify.class, null, modify -> !OptUtils.requiresJob(modify) && modify.isDelete(),
                        operandJ(
                                TableScan.class,
                                null,
                                scan -> OptUtils.hasTableType(scan, PartitionedMapTable.class),
                                none()
                        )
                ),
                DeleteByKeyMapLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableModify delete = call.rel(0);
        TableScan scan = call.rel(1);

        RelOptTable table = scan.getTable();
        RexNode keyCondition = OptUtils.extractKeyConstantExpression(table, delete.getCluster().getRexBuilder());
        if (keyCondition != null) {
            DeleteByKeyMapLogicalRel rel = new DeleteByKeyMapLogicalRel(
                    delete.getCluster(),
                    OptUtils.toLogicalConvention(delete.getTraitSet()),
                    table,
                    keyCondition
            );
            call.transformTo(rel);
        }
    }
}
