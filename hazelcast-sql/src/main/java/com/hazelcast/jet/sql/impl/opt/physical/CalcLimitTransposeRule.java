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

package com.hazelcast.jet.sql.impl.opt.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import static java.util.Collections.singletonList;

/**
 * Logical rule that transposes {@link LimitPhysicalRel} with {@link CalcPhysicalRel},
 * if filter is not present in Calc rex program.
 * <p>
 * Before:
 * <pre>
 * CalcPhysicalRel[...]
 *   LimitPhysicalRel[...]
 * </pre>
 * After:
 * <pre>
 * LimitPhysicalRel
 *   CalcPhysicalRel[...]
 * </pre>
 */
@Value.Enclosing
public class CalcLimitTransposeRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableCalcLimitTransposeRule.Config.builder()
                .description(CalcLimitTransposeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(CalcPhysicalRel.class)
                        .predicate(calc -> calc.getProgram().getCondition() == null)
                        .inputs(b1 -> b1
                                .operand(LimitPhysicalRel.class)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new CalcLimitTransposeRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new CalcLimitTransposeRule(Config.DEFAULT);

    protected CalcLimitTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        CalcPhysicalRel calc = call.rel(0);
        LimitPhysicalRel limit = call.rel(1);
        RelNode input = limit.getInput();

        CalcPhysicalRel newCalc = (CalcPhysicalRel) calc.copy(calc.getTraitSet(), input, calc.getProgram());
        LimitPhysicalRel newLimit = (LimitPhysicalRel) limit.copy(newCalc.getTraitSet(), singletonList(newCalc));

        call.transformTo(newLimit);
    }
}
