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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.NlsString;
import org.immutables.value.Value;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Value.Enclosing
public class ExtractGetDdlRule extends RelRule<RelRule.Config> {

    public static final RelOptRule INSTANCE = new ExtractGetDdlRule(Config.DEFAULT);

    private final GetDdlFunctionFinder finder = new GetDdlFunctionFinder();

    @Value.Immutable
    public interface Config extends RelRule.Config {
        ExtractGetDdlRule.Config DEFAULT = ImmutableExtractGetDdlRule.Config.builder()
                .description(ExtractGetDdlRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Calc.class)
                        .oneInput(b1 -> b1
                                .operand(Values.class)
                                .noInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new ExtractGetDdlRule(this);
        }
    }

    public ExtractGetDdlRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        rel.accept(finder);
        return finder.found;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        Values values = call.rel(1);

        List<RexNode> functionOperands = calc.getProgram().expandList(finder.functionOperands);
        assert functionOperands.stream().allMatch(rex -> rex instanceof RexLiteral);

        List<String> operands = functionOperands.stream()
                .map(rex -> ((RexLiteral) rex).getValue())
                .map(cmp -> ((NlsString) cmp).getValue())
                .collect(toList());

        call.transformTo(
                new GetDdlRel(
                        calc.getCluster(),
                        calc.getTraitSet(),
                        operands
                ));
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    static class GetDdlFunctionFinder extends RexShuttle {
        public List<RexNode> functionOperands;
        public boolean found;

        @Override
        public RexNode visitCall(RexCall call) {
            if (call.getOperator() == HazelcastSqlOperatorTable.GET_DDL) {
                functionOperands = call.getOperands();
                found = true;
            }
            return super.visitCall(call);
        }
    }
}
