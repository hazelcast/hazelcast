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

package com.hazelcast.jet.sql.impl.opt.nojobshortcuts;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.TableModifyLogicalRel;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

/**
 * Planner rule that matches single-key, constant expression,
 * {@link PartitionedMapTable} UPDATE.
 * <p>For example,</p>
 * <blockquote><code>UPDATE map SET this = 2 WHERE __key = 1</code></blockquote>
 * <p>
 * Such UPDATE is translated to optimized, direct key {@code IMap} operation
 * which does not involve starting any job.
 */
@Value.Enclosing
public final class UpdateByKeyMapRule extends RelRule<Config> {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableUpdateByKeyMapRule.Config.builder()
                .description(UpdateByKeyMapRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(TableModifyLogicalRel.class)
                        .trait(LOGICAL)
                        .predicate(modify -> !OptUtils.requiresJob(modify) && modify.isUpdate())
                        .inputs(b1 -> b1
                                .operand(FullScanLogicalRel.class)
                                .predicate(scan -> OptUtils.hasTableType(scan, PartitionedMapTable.class))
                                .noInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new UpdateByKeyMapRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new UpdateByKeyMapRule(Config.DEFAULT);

    private UpdateByKeyMapRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableModifyLogicalRel update = call.rel(0);
        FullScanLogicalRel scan = call.rel(1);

        RelOptTable table = scan.getTable();
        RexNode keyCondition = OptUtils.extractKeyConstantExpression(table, update.getCluster().getRexBuilder());
        if (keyCondition != null) {
            UpdateByKeyMapRel rel = new UpdateByKeyMapRel(
                    update.getCluster(),
                    OptUtils.toPhysicalConvention(update.getTraitSet()),
                    table,
                    keyCondition,
                    update.getUpdateColumnList(),
                    update.getSourceExpressionList()
            );
            call.transformTo(rel);
        }
    }
}
