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
 * Logical rule that transposes {@link DropLateItemsLogicalRel} with {@link Calc}.
 * <p>
 * Before:
 * <pre>
 * Calc[...]
 *   DropLateItemsRel[...]
 * </pre>
 * After:
 * <pre>
 * DropLateItemsRel
 *   Calc[...]
 * </pre>
 * <p>
 */
@Value.Enclosing
public class CalcDropLateItemsTransposeRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableCalcDropLateItemsTransposeRule.Config.builder()
                .description(CalcDropLateItemsTransposeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Calc.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(DropLateItemsLogicalRel.class)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new CalcDropLateItemsTransposeRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new CalcDropLateItemsTransposeRule(Config.DEFAULT);

    protected CalcDropLateItemsTransposeRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        DropLateItemsLogicalRel dropRel = call.rel(1);

        if (!(dropRel.wmField() instanceof RexInputRef)) {
            return false;
        }
        RexInputRef wmField = (RexInputRef) dropRel.wmField();

        List<RexNode> rexNodes = calc.getProgram().expandList(calc.getProgram().getProjectList());
        return rexNodes.stream()
                .filter(r -> r instanceof RexInputRef)
                .map(r -> (RexInputRef) r)
                .anyMatch(r -> r.getIndex() == wmField.getIndex());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        DropLateItemsLogicalRel dropRel = call.rel(1);
        RelNode input = dropRel.getInput();

        Calc newCalc = calc.copy(calc.getTraitSet(), input, calc.getProgram());
        // TODO[sasha]: calc may move watermarked field, so re-projection is needed.
        DropLateItemsLogicalRel newDropRel = dropRel.copy(dropRel.getTraitSet(), newCalc);

        call.transformTo(newDropRel);
    }
}
