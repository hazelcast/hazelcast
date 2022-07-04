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

import com.hazelcast.jet.sql.impl.opt.ExpressionValues;
import com.hazelcast.jet.sql.impl.opt.ExpressionValues.SimpleExpressionValues;
import com.hazelcast.jet.sql.impl.opt.ExpressionValues.TransformedExpressionValues;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Collections.singletonList;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.some;

final class ValuesLogicalRules {

    static final RelOptRuleOperand VALUES_CHILD_OPERAND = operand(ValuesLogicalRel.class, none());

    static final RelOptRule CONVERT_INSTANCE =
            new ConverterRule(
                    LogicalValues.class, Convention.NONE, LOGICAL,
                    ValuesLogicalRules.class.getSimpleName() + "(Convert)"
            ) {
                @Override
                public RelNode convert(RelNode rel) {
                    LogicalValues values = (LogicalValues) rel;
                    ExpressionValues expressionValues = new SimpleExpressionValues(values.getTuples());
                    return new ValuesLogicalRel(
                            values.getCluster(),
                            OptUtils.toLogicalConvention(values.getTraitSet()),
                            values.getRowType(),
                            singletonList(expressionValues)
                    );
                }
            };

    static final RelOptRule CALC_INSTANCE =
            new RelOptRule(
                    operand(CalcLogicalRel.class, some(operand(ValuesLogicalRel.class, none()))),
                    ValuesLogicalRules.class.getSimpleName() + "(Project)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    CalcLogicalRel calc = call.rel(0);
                    ValuesLogicalRel values = call.rel(1);

                    RexProgram rexProgram = calc.getProgram();

                    RexNode filter = null;
                    if (rexProgram.getCondition() != null) {
                        filter = rexProgram.expandLocalRef(rexProgram.getCondition());
                    }

                    ExpressionValues expressionValues = new TransformedExpressionValues(
                            filter,
                            rexProgram.expandList(rexProgram.getProjectList()),
                            values.getRowType(),
                            values.values(),
                            ((HazelcastRelOptCluster) calc.getCluster()).getParameterMetadata()
                    );
                    RelNode rel = new ValuesLogicalRel(
                            calc.getCluster(),
                            calc.getTraitSet(),
                            rexProgram.getOutputRowType(),
                            singletonList(expressionValues)
                    );
                    call.transformTo(rel);
                }
            };

    static final RelOptRule UNION_INSTANCE =
            new RelOptRule(
                    operand(UnionLogicalRel.class, RelOptRule.unordered(VALUES_CHILD_OPERAND)),
                    ValuesLogicalRules.class.getSimpleName() + "(Union)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Union union = call.rel(0);
                    List<ExpressionValues> expressionValues = new ArrayList<>(union.getInputs().size());
                    for (RelNode input : union.getInputs()) {
                        ValuesLogicalRel values = OptUtils.findMatchingRel(input, VALUES_CHILD_OPERAND);
                        if (values == null) {
                            return;
                        }
                        expressionValues.addAll(values.values());
                    }
                    RelNode rel = new ValuesLogicalRel(
                            union.getCluster(),
                            union.getTraitSet(),
                            union.getRowType(),
                            expressionValues
                    );
                    call.transformTo(rel);
                }
            };

    private ValuesLogicalRules() {
    }
}
