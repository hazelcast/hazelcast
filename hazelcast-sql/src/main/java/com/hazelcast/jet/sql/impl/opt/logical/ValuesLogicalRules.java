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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Collections.singletonList;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.some;
import static org.apache.calcite.plan.RelOptRule.unordered;

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

    static final RelOptRule FILTER_INSTANCE =
            new RelOptRule(
                    operand(FilterLogicalRel.class, some(operand(ValuesLogicalRel.class, none()))),
                    ValuesLogicalRules.class.getSimpleName() + "(Filter)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Filter filter = call.rel(0);
                    ValuesLogicalRel values = call.rel(1);
                    ExpressionValues expressionValues = new TransformedExpressionValues(
                            filter.getCondition(),
                            null,
                            values.getRowType(),
                            values.values(),
                            ((HazelcastRelOptCluster) filter.getCluster()).getParameterMetadata()
                    );
                    RelNode rel = new ValuesLogicalRel(
                            filter.getCluster(),
                            filter.getTraitSet(),
                            filter.getRowType(),
                            singletonList(expressionValues)
                    );
                    call.transformTo(rel);
                }
            };

    static final RelOptRule PROJECT_INSTANCE =
            new RelOptRule(
                    operand(ProjectLogicalRel.class, some(operand(ValuesLogicalRel.class, none()))),
                    ValuesLogicalRules.class.getSimpleName() + "(Project)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    ValuesLogicalRel values = call.rel(1);
                    ExpressionValues expressionValues = new TransformedExpressionValues(
                            null,
                            project.getProjects(),
                            values.getRowType(),
                            values.values(),
                            ((HazelcastRelOptCluster) project.getCluster()).getParameterMetadata()
                    );
                    RelNode rel = new ValuesLogicalRel(
                            project.getCluster(),
                            project.getTraitSet(),
                            project.getRowType(),
                            singletonList(expressionValues)
                    );
                    call.transformTo(rel);
                }
            };

    static final RelOptRule PROJECT_FILTER_INSTANCE =
            new RelOptRule(
                    operand(ProjectLogicalRel.class,
                            some(operand(FilterLogicalRel.class, operand(ValuesLogicalRel.class, none())))),
                    ValuesLogicalRules.class.getSimpleName() + "(Project-Filter)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Filter filter = call.rel(1);
                    ValuesLogicalRel values = call.rel(2);
                    ExpressionValues expressionValues = new TransformedExpressionValues(
                            filter.getCondition(),
                            project.getProjects(),
                            values.getRowType(),
                            values.values(),
                            ((HazelcastRelOptCluster) project.getCluster()).getParameterMetadata()
                    );
                    RelNode rel = new ValuesLogicalRel(
                            project.getCluster(),
                            project.getTraitSet(),
                            project.getRowType(),
                            singletonList(expressionValues)
                    );
                    call.transformTo(rel);
                }
            };

    static final RelOptRule UNION_INSTANCE =
            new RelOptRule(
                    operand(UnionLogicalRel.class, unordered(VALUES_CHILD_OPERAND)),
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
