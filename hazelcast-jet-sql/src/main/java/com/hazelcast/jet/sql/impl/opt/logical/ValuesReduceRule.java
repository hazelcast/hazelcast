/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;

abstract class ValuesReduceRule extends RelOptRule {

    static final ValuesReduceRule FILTER_INSTANCE =
            new ValuesReduceRule(
                    operand(LogicalFilter.class, operand(LogicalValues.class, none())),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRule.class.getSimpleName() + "(Filter)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Filter filter = call.rel(0);
                    Values values = call.rel(1);
                    apply(call, null, filter, values);
                }
            };

    static final ValuesReduceRule PROJECT_INSTANCE =
            new ValuesReduceRule(
                    operand(LogicalProject.class, operand(LogicalValues.class, none())),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRule.class.getSimpleName() + "(Project)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Values values = call.rel(1);
                    apply(call, project, null, values);
                }
            };

    static final ValuesReduceRule PROJECT_FILTER_INSTANCE =
            new ValuesReduceRule(
                    operand(LogicalProject.class, operand(LogicalFilter.class, operand(LogicalValues.class, none()))),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRule.class.getSimpleName() + "(Project-Filter)"
            ) {
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Filter filter = call.rel(1);
                    Values values = call.rel(2);
                    apply(call, project, filter, values);
                }
            };

    private ValuesReduceRule(
            RelOptRuleOperand operand,
            RelBuilderFactory relBuilderFactory,
            String description
    ) {
        super(operand, relBuilderFactory, description);
    }

    @SuppressWarnings("unchecked")
    protected void apply(
            RelOptRuleCall call,
            Project project,
            Filter filter,
            Values values
    ) {
        PlanNodeSchema schema = OptUtils.schema(values.getRowType());
        RexVisitor<Expression<?>> converter = OptUtils.createRexToExpressionVisitor(schema);

        RelDataType rowType = null;

        Expression<Boolean> predicate = null;
        if (filter != null) {
            rowType = filter.getRowType();
            predicate = (Expression<Boolean>) filter.getCondition().accept(converter);
        }
        List<Expression<?>> projection = null;
        if (project != null) {
            rowType = project.getRowType();
            projection = toList(project.getProjects(), node -> node.accept(converter));
        }

        assert rowType != null;

        List<Object[]> rows = ExpressionUtil.evaluate(predicate, projection, OptUtils.convert(values));
        ImmutableList<ImmutableList<RexLiteral>> tuples = toTuples(rows, rowType.getFieldList(), values.getCluster());

        LogicalValues rel = LogicalValues.create(
                values.getCluster(),
                rowType,
                tuples
        );
        call.transformTo(rel);
    }

    private static ImmutableList<ImmutableList<RexLiteral>> toTuples(
            List<Object[]> rows,
            List<RelDataTypeField> fields,
            RelOptCluster cluster
    ) {
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RexBuilder rexBuilder = cluster.getRexBuilder();

        ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = new ImmutableList.Builder<>();
        for (Object[] row : rows) {
            ImmutableList.Builder<RexLiteral> tupleBuilder = new ImmutableList.Builder<>();
            for (int i = 0; i < row.length; i++) {
                assert row.length == fields.size();

                RelDataType relDataType = typeFactory.createTypeWithNullability(fields.get(i).getType(), false);
                RexLiteral literal = (RexLiteral) rexBuilder.makeLiteral(row[i], relDataType, false);
                tupleBuilder.add(literal);
            }
            ImmutableList<RexLiteral> tuple = tupleBuilder.build();

            tuplesBuilder.add(tuple);
        }
        return tuplesBuilder.build();
    }
}
