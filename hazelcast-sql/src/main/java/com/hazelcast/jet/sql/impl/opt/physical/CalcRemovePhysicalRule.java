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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.physical.CalcRemovePhysicalRule.Config;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.immutables.value.Value;

// Copied from CalcRemovePhysicalRule.
// Removes trivial Calc physical relation.
@Value.Enclosing
public class CalcRemovePhysicalRule extends RelRule<Config> implements SubstitutionRule {
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableCalcRemovePhysicalRule.Config.builder()
                .description(CalcRemovePhysicalRule.class.getSimpleName())
                .operandSupplier(b ->
                        b.operand(CalcPhysicalRel.class)
                                .predicate(calc -> calc.getProgram().isTrivial())
                                .anyInputs())
                .build();

        @Override
        default CalcRemovePhysicalRule toRule() {
            return new CalcRemovePhysicalRule(this);
        }
    }

    public static final CalcRemovePhysicalRule INSTANCE = new CalcRemovePhysicalRule(Config.DEFAULT);

    protected CalcRemovePhysicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final CalcPhysicalRel calc = call.rel(0);
        assert calc.getProgram().isTrivial() : "rule predicate";
        RelNode input = calc.getInput();
        input = call.getPlanner().register(input, calc);
        call.transformTo(convert(input, calc.getTraitSet()));
    }
}