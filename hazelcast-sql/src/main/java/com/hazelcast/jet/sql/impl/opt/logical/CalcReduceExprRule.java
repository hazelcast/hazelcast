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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule.CalcReduceExpressionsRule.CalcReduceExpressionsRuleConfig;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Copy of Calcite {@link CalcReduceExpressionsRule} with added workaround for a bug
 * <a href="https://issues.apache.org/jira/browse/CALCITE-5285">CALCITE-5285</a>.
 */
public final class CalcReduceExprRule extends ReduceExpressionsRule<CalcReduceExpressionsRuleConfig> {
    public static final CalcReduceExprRule INSTANCE = new CalcReduceExprRule();

    private CalcReduceExprRule() {
        super(CalcReduceExpressionsRuleConfig.DEFAULT);
    }

    @Override
    @SuppressWarnings("EmptyBlock")
    public void onMatch(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        RexProgram program = calc.getProgram();
        final List<RexNode> exprList = program.getExprList();

        // Form a list of expressions with sub-expressions fully expanded.
        final List<RexNode> expandedExprList = new ArrayList<>();
        final RexShuttle shuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitLocalRef(RexLocalRef localRef) {
                        return expandedExprList.get(localRef.getIndex());
                    }
                };
        for (RexNode expr : exprList) {
            expandedExprList.add(expr.accept(shuttle));
        }
        final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
        if (reduceExpressions(calc, expandedExprList, predicates, false,
                config.matchNullability(), config.treatDynamicCallsAsConstant())) {
            final RexProgramBuilder builder =
                    new RexProgramBuilder(
                            calc.getInput().getRowType(),
                            calc.getCluster().getRexBuilder());
            final List<RexLocalRef> list = new ArrayList<>();
            for (RexNode expr : expandedExprList) {
                list.add(builder.registerInput(expr));
            }
            if (program.getCondition() != null) {
                final int conditionIndex =
                        program.getCondition().getIndex();
                final RexNode newConditionExp =
                        expandedExprList.get(conditionIndex);
                if (newConditionExp.isAlwaysTrue()) {
                    // condition is always TRUE - drop it.
                } else if (newConditionExp instanceof RexLiteral
                        || RexUtil.isNullLiteral(newConditionExp, true)) {
                    // condition is always NULL or FALSE - replace calc
                    // with empty.
                    call.transformTo(createEmptyRelOrEquivalent(call, calc));
                    return;
                } else {
                    builder.addCondition(list.get(conditionIndex));
                }
            }
            int k = 0;
            for (RexLocalRef projectExpr : program.getProjectList()) {
                final int index = projectExpr.getIndex();
                builder.addProject(
                        list.get(index).getIndex(),
                        program.getOutputRowType().getFieldNames().get(k++));
            }
            Calc newCalc = calc.copy(calc.getTraitSet(), calc.getInput(), builder.getProgram());
            if (newCalc.deepEquals(calc)) {
                return;
            }
            call.transformTo(newCalc);

            // New plan is absolutely better than old plan.
            call.getPlanner().prune(calc);
        }
    }

    protected RelNode createEmptyRelOrEquivalent(RelOptRuleCall call, Calc input) {
        return call.builder().push(input).empty().build();
    }
}
