/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.common;

import com.hazelcast.jet.sql.impl.opt.FullScan;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.util.Permutation;
import org.immutables.value.Value;

import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Arrays.asList;
import static org.apache.calcite.rex.RexUtil.EXECUTOR;

/**
 * Logical rule that pushes a {@link Calc} down into a {@link TableScan} to allow for constrained scans.
 * See {@link HazelcastTable} for more information about constrained scans.
 * <p>
 * Before:
 * <pre>
 * Calc[filter=exp1]
 *     TableScan[table[filter=exp2]]
 * </pre>
 * After:
 * <pre>
 * TableScan[table[filter=exp1 AND exp2]]
 * </pre>
 */
@Value.Enclosing
public final class CalcIntoScanRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        CalcIntoScanRule.Config DEFAULT = ImmutableCalcIntoScanRule.Config.builder()
                .description(CalcIntoScanRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Calc.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(FullScan.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new CalcIntoScanRule(this);
        }
    }

    public static final CalcIntoScanRule INSTANCE = new CalcIntoScanRule(Config.DEFAULT);

    private CalcIntoScanRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // we don't merge projections. Refuse to match the rule if the scan's projection isn't identity.
        FullScan scan = call.rel(1);
        HazelcastTable table = OptUtils.extractHazelcastTable(scan);
        Permutation permutation = Project.getPermutation(table.getTarget().getFieldCount(), table.getProjects());
        return permutation != null && permutation.isIdentity();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        FullScan scan = call.rel(1);

        HazelcastTable table = OptUtils.extractHazelcastTable(scan);
        RexProgram program = calc.getProgram();
        assert scan.getConvention() == LOGICAL; // support it for physical rels also.

        List<RexNode> newProjects = program.expandList(program.getProjectList());
        HazelcastTable newTable = table.withProject(newProjects, program.getOutputRowType());

        // merge filters
        if (program.getCondition() != null) {
            RexNode calcFilter = program.expandLocalRef(program.getCondition());
            RexNode scanFilter = table.getFilter();

            RexSimplify rexSimplify = new RexSimplify(
                    call.builder().getRexBuilder(),
                    RelOptPredicateList.EMPTY,
                    EXECUTOR);
            RexNode simplifiedCondition = rexSimplify.simplifyFilterPredicates(asList(calcFilter, scanFilter));
            newTable = newTable.withFilter(simplifiedCondition);
        }

        HazelcastRelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                newTable,
                scan.getCluster().getTypeFactory()
        );

        call.transformTo(new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                convertedTable,
                scan.lagExpression(),
                OptUtils.getTargetField(program, scan.watermarkedColumnIndex())
        ));
    }
}
