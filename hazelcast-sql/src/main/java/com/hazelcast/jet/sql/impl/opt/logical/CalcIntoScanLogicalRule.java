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
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.immutables.value.Value;

import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

/**
 * Logical rule that pushes a {@link Calc} down into a {@link TableScan} to allow for constrained scans.
 * See {@link HazelcastTable} for more information about constrained scans.
 * <p>
 * Before:
 * <pre>
 * LogicalCalc[filter=exp1]
 *     LogicalScan[table[filter=exp2]]
 * </pre>
 * After:
 * <pre>
 * LogicalScan[table[filter=exp1 AND exp2]]
 * </pre>
 */
@Value.Enclosing
public final class CalcIntoScanLogicalRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        CalcIntoScanLogicalRule.Config DEFAULT = ImmutableCalcIntoScanLogicalRule.Config.builder()
                .description(CalcIntoScanLogicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(CalcLogicalRel.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(FullScanLogicalRel.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new CalcIntoScanLogicalRule(this);
        }
    }

    public static final CalcIntoScanLogicalRule INSTANCE = new CalcIntoScanLogicalRule(Config.DEFAULT);

    private CalcIntoScanLogicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        CalcLogicalRel calc = call.rel(0);
        FullScanLogicalRel scan = call.rel(1);

        HazelcastTable table = OptUtils.extractHazelcastTable(scan);

        RexProgram program = calc.getProgram();

        List<RexNode> newProjects = program.expandList(program.getProjectList());
        HazelcastTable newTable = table.withProject(newProjects, program.getOutputRowType());

        if (program.getCondition() != null) {
            newTable = newTable.withFilter(program.expandLocalRef(program.getCondition()));
        }

        HazelcastRelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                newTable,
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
