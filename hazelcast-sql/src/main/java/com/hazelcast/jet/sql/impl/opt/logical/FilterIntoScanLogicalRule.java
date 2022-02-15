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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.Arrays;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.inlineExpression;

/**
 * Logical rule that pushes down a {@link Filter} into a {@link TableScan} to allow for constrained scans.
 * See {@link HazelcastTable} for more information about constrained scans.
 * <p>
 * Before:
 * <pre>
 * LogicalFilter[filter=exp1]
 *     LogicalScan[table[filter=exp2]]
 * </pre>
 * After:
 * <pre>
 * LogicalScan[table[filter=exp1 AND exp2]]
 * </pre>
 */
public final class FilterIntoScanLogicalRule extends RelRule<Config> implements TransformationRule {

    private static final Config CONFIG = Config.EMPTY
            .withDescription(FilterIntoScanLogicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0
                    .operand(Filter.class)
                    .trait(LOGICAL)
                    .inputs(b1 -> b1
                            .operand(FullScanLogicalRel.class).anyInputs()));

    public static final RelOptRule INSTANCE = new FilterIntoScanLogicalRule(CONFIG);

    private FilterIntoScanLogicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        FullScanLogicalRel scan = call.rel(1);

        HazelcastTable table = OptUtils.extractHazelcastTable(scan);
        RexNode existingCondition = table.getFilter();

        // inline the table projections into the merged condition. For example, if we have this relexp:
        //   Filter[$0=?]
        //     Scan[projects=[$1 + $2]
        // The filter condition will be converted to:
        //   [$1 + $2 = ?]
        RexNode convertedCondition = inlineExpression(table.getProjects(), filter.getCondition());
        if (existingCondition != null) {
            convertedCondition = RexUtil.composeConjunction(
                    scan.getCluster().getRexBuilder(),
                    Arrays.asList(existingCondition, convertedCondition),
                    true
            );
        }

        HazelcastRelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                table.withFilter(convertedCondition),
                scan.getCluster().getTypeFactory()
        );

        FullScanLogicalRel rel = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                convertedTable,
                scan.eventTimePolicyProvider(),
                scan.watermarkedColumnIndex()
        );
        call.transformTo(rel);
    }
}
