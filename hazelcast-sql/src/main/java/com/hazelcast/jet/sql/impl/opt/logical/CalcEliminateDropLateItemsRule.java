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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

/**
 * Logical rule that eliminates {@link DropLateItemsLogicalRel}
 * under {@link Calc}, if calc doesn't project watermarked field.
 * <p>
 * Before:
 * <pre>
 * Calc[...]
 *   DropLateItemsRel[...]
 * </pre>
 * After:
 * <pre>
 * Calc[...]
 * </pre>
 * <p>
 */
@Value.Enclosing
public class CalcEliminateDropLateItemsRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableCalcEliminateDropLateItemsRule.Config.builder()
                .description(CalcEliminateDropLateItemsRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Calc.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(DropLateItemsLogicalRel.class)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new CalcEliminateDropLateItemsRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new CalcEliminateDropLateItemsRule(Config.DEFAULT);

    protected CalcEliminateDropLateItemsRule(Config config) {
        super(config);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public boolean matches(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        DropLateItemsLogicalRel dropRel = call.rel(1);

        int wmField = dropRel.wmField();

        List<RexNode> rexNodes = calc.getProgram().expandList(calc.getProgram().getProjectList());
        return rexNodes.stream()
                .filter(r -> r instanceof RexInputRef)
                .map(r -> (RexInputRef) r)
                .noneMatch(r -> r.getIndex() == wmField);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        DropLateItemsLogicalRel dropRel = call.rel(1);
        RelNode input = dropRel.getInput();

        call.transformTo(calc.copy(calc.getTraitSet(), input, calc.getProgram()));
    }
}
