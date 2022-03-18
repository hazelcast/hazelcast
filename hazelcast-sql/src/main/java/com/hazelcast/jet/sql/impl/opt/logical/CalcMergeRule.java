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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;

@Value.Enclosing
public final class CalcMergeRule extends RelRule<Config> {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        CalcMergeRule.Config DEFAULT = ImmutableCalcMergeRule.Config.builder()
                .description(CalcMergeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Calc.class)
                        .inputs(b1 -> b1
                                .operand(Calc.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new CalcMergeRule(this);
        }
    }

    private CalcMergeRule(Config config) {
        super(config);
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    public static final RelOptRule INSTANCE = new CalcMergeRule(Config.DEFAULT);

    @Override
    public boolean matches(RelOptRuleCall call) {
        Calc upperCalc = call.rel(0);
        Calc lowerCalc = call.rel(1);

        RexProgram upperCalcProgram = upperCalc.getProgram();
        RexProgram lowerCalcProgram = lowerCalc.getProgram();

        List<ImmutableBitSet> upperCalcInputIndices = upperCalcProgram
                .getProjectList()
                .stream()
                .map(upperCalcProgram::expandLocalRef)
                .map(InputFinder::bits)
                .collect(Collectors.toList());

        ImmutableBitSet upperFilterIndices;
        if (upperCalcProgram.getCondition() != null) {
            upperFilterIndices = InputFinder.bits(upperCalcProgram.expandLocalRef(upperCalcProgram.getCondition()));
        } else {
            upperFilterIndices = ImmutableBitSet.of();
        }

        upperCalcInputIndices.add(upperFilterIndices);

        List<RexNode> lowerCalcProjects = lowerCalcProgram.getProjectList()
                .stream()
                .map(lowerCalcProgram::expandLocalRef)
                .collect(Collectors.toList());

        boolean isMergeable = true;

        for (int i = 0; i < lowerCalcProjects.size(); ++i) {
            RexNode project = lowerCalcProjects.get(i);
            int nonDeterministicRexRefCount = 0;
            if (!RexUtil.isDeterministic(project)) {
                for (ImmutableBitSet bitSet : upperCalcInputIndices) {
                    for (Integer ref : bitSet) {
                        if (ref == i) {
                            nonDeterministicRexRefCount++;
                        }
                    }
                }
            }
            isMergeable &= nonDeterministicRexRefCount <= 1;
        }

        return isMergeable;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Calc upperCalc = call.rel(0);
        Calc lowerCalc = call.rel(1);

        RexProgram topProgram = upperCalc.getProgram();
        RexBuilder rexBuilder = upperCalc.getCluster().getRexBuilder();
        // Merge the programs together.
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
                upperCalc.getProgram(), lowerCalc.getProgram(), rexBuilder);

        assert mergedProgram.getOutputRowType().equals(topProgram.getOutputRowType());

        // TODO: optimize filter expression
        Calc newCalc = upperCalc.copy(upperCalc.getTraitSet(), lowerCalc.getInput(), mergedProgram);
        if (newCalc.getDigest().equals(lowerCalc.getDigest())) {
            call.getPlanner().prune(upperCalc);
        }
        call.transformTo(newCalc);
    }
}
