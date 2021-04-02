/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.ExpressionValues;
import com.hazelcast.jet.sql.impl.opt.ExpressionValues.CompoundExpressionValues;
import com.hazelcast.jet.sql.impl.opt.ExpressionValues.SimpleExpressionValues;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static java.util.Collections.singletonList;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.unordered;

final class ValuesLogicalRules {

    static final RelOptRuleOperand VALUES_CHILD_OPERAND = operand(Values.class, none());

    static final RelOptRule CONVERT_INSTANCE =
            new ConverterRule(
                    LogicalValues.class, Convention.NONE, Convention.NONE,
                    ValuesLogicalRules.class.getSimpleName() + "(Convert)"
            ) {
                @Override
                public RelNode convert(RelNode rel) {
                    LogicalValues values = (LogicalValues) rel;
                    ExpressionValues expressionValues = new SimpleExpressionValues(values.getTuples());
                    return new Values(
                            values.getCluster(),
                            values.getTraitSet(),
                            values.getRowType(),
                            singletonList(expressionValues)
                    );
                }
            };

    static final RelOptRule FILTER_INSTANCE =
            new RelOptRule(
                    operand(LogicalFilter.class, operand(Values.class, none())),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesLogicalRules.class.getSimpleName() + "(Filter)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Filter filter = call.rel(0);
                    Values values = call.rel(1);
                    ExpressionValues expressionValues = new CompoundExpressionValues(
                            filter.getCondition(),
                            null,
                            values.getRowType(),
                            values.values()
                    );
                    RelNode rel = new Values(
                            filter.getCluster(),
                            filter.getRowType(),
                            singletonList(expressionValues)
                    );
                    call.transformTo(rel);
                }
            };

    static final RelOptRule PROJECT_INSTANCE =
            new RelOptRule(
                    operand(LogicalProject.class, operand(Values.class, none())),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesLogicalRules.class.getSimpleName() + "(Project)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Values values = call.rel(1);
                    ExpressionValues expressionValues = new CompoundExpressionValues(
                            null,
                            project.getProjects(),
                            values.getRowType(),
                            values.values()
                    );
                    RelNode rel = new Values(
                            project.getCluster(),
                            project.getRowType(),
                            singletonList(expressionValues)
                    );
                    call.transformTo(rel);
                }
            };

    static final RelOptRule PROJECT_FILTER_INSTANCE =
            new RelOptRule(
                    operand(LogicalProject.class, operand(LogicalFilter.class, operand(Values.class, none()))),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesLogicalRules.class.getSimpleName() + "(Project-Filter)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Filter filter = call.rel(1);
                    Values values = call.rel(2);
                    ExpressionValues expressionValues = new CompoundExpressionValues(
                            filter.getCondition(),
                            project.getProjects(),
                            values.getRowType(),
                            values.values()
                    );
                    RelNode rel = new Values(
                            project.getCluster(),
                            project.getRowType(),
                            singletonList(expressionValues)
                    );
                    call.transformTo(rel);
                }
            };

    static final RelOptRule UNION_INSTANCE =
            new RelOptRule(
                    operand(LogicalUnion.class, unordered(VALUES_CHILD_OPERAND)),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesLogicalRules.class.getSimpleName() + "(Union)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Union union = call.rel(0);

                    List<ExpressionValues> expressionValues = new ArrayList<>(union.getInputs().size());
                    for (RelNode input : union.getInputs()) {
                        Values values = OptUtils.findMatchingRel(input, VALUES_CHILD_OPERAND);
                        expressionValues.addAll(values.values());
                    }

                    RelNode rel = new Values(
                            union.getCluster(),
                            union.getRowType(),
                            expressionValues
                    );
                    call.transformTo(rel);
                }
            };

    static final RelOptRule INSTANCE =
            new ConverterRule(
                    Values.class, Convention.NONE, LOGICAL,
                    ValuesLogicalRules.class.getSimpleName()
            ) {
                @Override
                public RelNode convert(RelNode rel) {
                    Values values = (Values) rel;

                    return new ValuesLogicalRel(
                            values.getCluster(),
                            OptUtils.toLogicalConvention(values.getTraitSet()),
                            values.getRowType(),
                            values.values()
                    );
                }
            };

    private static final class Values extends AbstractRelNode {

        private final List<ExpressionValues> values;

        private Values(RelOptCluster cluster, RelDataType rowType, List<ExpressionValues> values) {
            this(cluster, cluster.traitSetOf(Convention.NONE), rowType, values);
        }

        private Values(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, List<ExpressionValues> values) {
            super(cluster, traits);

            this.rowType = rowType;
            this.values = values;
        }

        public List<ExpressionValues> values() {
            return values;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Values(getCluster(), getRowType(), values);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return pw.item("id", this.id);
        }
    }

    private ValuesLogicalRules() {
    }
}
