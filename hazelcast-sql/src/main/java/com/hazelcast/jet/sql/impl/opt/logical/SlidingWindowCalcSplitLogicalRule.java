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

import com.hazelcast.jet.sql.impl.opt.SlidingWindow;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexVisitorImpl;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.logical.SlidingWindowCalcSplitLogicalRule.Config.DEFAULT;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rex.RexProgramBuilder.forProgram;

/**
 * A {@link Calc} condition reading from a {@link SlidingWindow}
 * will be moved before the sliding window as another {@link Calc}
 */
@Value.Enclosing
public class SlidingWindowCalcSplitLogicalRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableSlidingWindowCalcSplitLogicalRule.Config.builder()
                .description(SlidingWindowCalcSplitLogicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(CalcLogicalRel.class)
                        .predicate(calc -> calc.getProgram().getCondition() != null)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(SlidingWindow.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new SlidingWindowCalcSplitLogicalRule(this);
        }
    }

    public static final RelOptRule STREAMING_FILTER_TRANSPOSE = new SlidingWindowCalcSplitLogicalRule(DEFAULT);

    protected SlidingWindowCalcSplitLogicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final CalcLogicalRel calc = call.rel(0);
        final SlidingWindow sw = call.rel(1);

        final boolean[] mustNotExecute = {false};

        RexProgram program = calc.getProgram();
        RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef ref) {
                int index = ref.getIndex();
                if (index == sw.windowStartIndex() || index == sw.windowEndIndex()) {
                    mustNotExecute[0] = true;
                }
                return super.visitInputRef(ref);
            }
        };

        program.expandLocalRef(program.getCondition()).accept(visitor);

        if (mustNotExecute[0]) {
            call.transformTo(new MustNotExecuteLogicalRel(
                    sw.getCluster(),
                    sw.getTraitSet(),
                    program.getOutputRowType(),
                    "Can't apply filter criteria to window bounds"
            ));
            return;
        }

        RexProgramBuilder programBuilder = new RexProgramBuilder(
                sw.getInput().getRowType(),
                sw.getCluster().getRexBuilder());
        programBuilder.clearCondition();
        programBuilder.addCondition(program.expandLocalRef(program.getCondition()));
        programBuilder.addIdentity();

        RexProgram identityProgram = programBuilder.getProgram();

        final CalcLogicalRel filterCalc = new CalcLogicalRel(
                calc.getCluster(),
                sw.getTraitSet(),
                calc.getHints(),
                sw.getInput(),
                identityProgram
        );

        RexProgramBuilder builder = forProgram(program, calc.getCluster().getRexBuilder(), true);
        builder.clearCondition();

        final SlidingWindow topSW = (SlidingWindow) sw.copy(sw.getTraitSet(), singletonList(filterCalc));
        final CalcLogicalRel newCalc = (CalcLogicalRel) calc.copy(calc.getTraitSet(), topSW, builder.getProgram());
        call.transformTo(newCalc);
    }
}
