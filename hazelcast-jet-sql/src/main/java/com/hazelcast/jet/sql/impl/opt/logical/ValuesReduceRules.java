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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.NOT_IMPLEMENTED_ARGUMENTS_CONTEXT;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.unordered;

final class ValuesReduceRules {

    static final RelOptRuleOperand REDUCED_VALUES_CHILD_OPERAND = operand(ReducedLogicalValues.class, none());

    static final RelOptRule FILTER_INSTANCE =
            new RelOptRule(
                    operand(LogicalFilter.class, operand(LogicalValues.class, none())),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRules.class.getSimpleName() + "(Filter)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Filter filter = call.rel(0);
                    Values values = call.rel(1);
                    RelNode rel = new ReducedLogicalValues(
                            filter.getCluster(),
                            filter.getRowType(),
                            filter.getCondition(),
                            null,
                            values.getTuples()
                    );
                    call.transformTo(rel);
                    call.getPlanner().prune(filter);
                }
            };

    static final RelOptRule PROJECT_INSTANCE =
            new RelOptRule(
                    operand(LogicalProject.class, operand(LogicalValues.class, none())),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRules.class.getSimpleName() + "(Project)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Values values = call.rel(1);
                    RelNode rel = new ReducedLogicalValues(
                            project.getCluster(),
                            project.getRowType(),
                            null,
                            project.getProjects(),
                            values.getTuples()
                    );
                    call.transformTo(rel);
                    call.getPlanner().prune(project);
                }
            };

    static final RelOptRule PROJECT_FILTER_INSTANCE =
            new RelOptRule(
                    operand(LogicalProject.class, operand(LogicalFilter.class, operand(LogicalValues.class, none()))),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRules.class.getSimpleName() + "(Project-Filter)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Filter filter = call.rel(1);
                    Values values = call.rel(2);
                    RelNode rel = new ReducedLogicalValues(
                            project.getCluster(),
                            project.getRowType(),
                            filter.getCondition(),
                            project.getProjects(),
                            values.getTuples()
                    );
                    call.transformTo(rel);
                    call.getPlanner().prune(project);
                }
            };

    static final RelOptRule UNION_INSTANCE =
            new RelOptRule(
                    operand(LogicalUnion.class, unordered(REDUCED_VALUES_CHILD_OPERAND)),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRules.class.getSimpleName() + "(Union)"
            ) {
                @Override
                @SuppressWarnings("unchecked")
                public void onMatch(RelOptRuleCall call) {
                    Union union = call.rel(0);

                    PlanNodeSchema schema = OptUtils.schema(union.getRowType());
                    RexVisitor<Expression<?>> converter = OptUtils.createRexToExpressionVisitor(schema);

                    List<Object[]> rows = new ArrayList<>();
                    for (RelNode input : union.getInputs()) {
                        ReducedLogicalValues values = OptUtils.findMatchingRel(input, REDUCED_VALUES_CHILD_OPERAND);

                        RexNode filter = values.filter();
                        Expression<Boolean> predicate = filter == null
                                ? null
                                : (Expression<Boolean>) filter.accept(converter);

                        List<RexNode> project = values.project();
                        List<Expression<?>> projection = project == null
                                ? null
                                : toList(project, node -> node.accept(converter));

                        List<Object[]> row = ExpressionUtil.evaluate(
                                predicate,
                                projection,
                                OptUtils.convert(values.tuples()),
                                NOT_IMPLEMENTED_ARGUMENTS_CONTEXT
                        );
                        rows.addAll(row);
                    }

                    RelNode rel = new ValuesLogicalRel(
                            union.getCluster(),
                            OptUtils.toLogicalConvention(union.getTraitSet()),
                            union.getRowType(),
                            rows
                    );
                    call.transformTo(rel);
                }
            };

    private static final class ReducedLogicalValues extends AbstractRelNode {

        private final RelDataType rowType;
        private final RexNode filter;
        private final List<RexNode> project;
        private final ImmutableList<ImmutableList<RexLiteral>> tuples;

        private ReducedLogicalValues(
                RelOptCluster cluster,
                RelDataType rowType,
                RexNode filter,
                List<RexNode> project,
                ImmutableList<ImmutableList<RexLiteral>> tuples
        ) {
            super(cluster, cluster.traitSetOf(Convention.NONE));

            this.rowType = rowType;
            this.filter = filter;
            this.project = project;
            this.tuples = tuples;
        }

        private RexNode filter() {
            return filter;
        }

        private List<RexNode> project() {
            return project;
        }

        private ImmutableList<ImmutableList<RexLiteral>> tuples() {
            return tuples;
        }

        @Override
        protected RelDataType deriveRowType() {
            return rowType;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new ReducedLogicalValues(getCluster(), rowType, filter, project, tuples);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return pw.item("id", this.id);
        }
    }

    private ValuesReduceRules() {
    }
}
