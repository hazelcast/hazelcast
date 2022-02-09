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

package com.hazelcast.jet.sql.impl.opt;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpression;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.row.EmptyRow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toList;

/**
 * Representation of the VALUES clause data, see subclasses.
 */
public abstract class ExpressionValues implements Serializable {

    public abstract int size();

    public abstract Stream<JetSqlRow> toValues(ExpressionEvalContext context);

    /**
     * Representation of the VALUES clause data in the form of a simple {@code
     * List<List<Expression>>}.
     */
    public static class SimpleExpressionValues extends ExpressionValues {

        private final List<List<? extends Expression<?>>> expressions;

        public SimpleExpressionValues(ImmutableList<ImmutableList<RexLiteral>> tuples) {
            this.expressions = tuples.stream()
                    .map(tuple -> tuple.stream().map(RexToExpression::convertLiteral).collect(toList()))
                    .collect(toList());
        }

        @Override
        public int size() {
            return expressions.size();
        }

        @Override
        public Stream<JetSqlRow> toValues(ExpressionEvalContext context) {
            return expressions.stream()
                    .map(es -> new JetSqlRow(context.getSerializationService(),
                            es.stream().map(e -> e.eval(EmptyRow.INSTANCE, context)).toArray(Object[]::new)));
        }

        @Override
        public String toString() {
            return "{expressions=" + expressions + "}";
        }
    }

    /**
     * A class that wraps another {@link ExpressionValues}, but applies an
     * optional filter and projection on top of them.
     */
    public static class TransformedExpressionValues extends ExpressionValues {

        private final Expression<Boolean> predicate;
        private final List<Expression<?>> projection;
        private final List<ExpressionValues> values;

        @SuppressWarnings("unchecked")
        public TransformedExpressionValues(
                RexNode filter,
                List<RexNode> project,
                RelDataType tuplesType,
                List<ExpressionValues> values,
                QueryParameterMetadata parameterMetadata
        ) {
            PlanNodeSchema schema = OptUtils.schema(tuplesType);
            RexVisitor<Expression<?>> converter =
                    OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);

            this.predicate = filter == null ? null : (Expression<Boolean>) filter.accept(converter);
            this.projection = project == null ? null : toList(project, node -> node.accept(converter));
            this.values = values;
        }

        @Override
        public int size() {
            return values.stream().mapToInt(ExpressionValues::size).sum();
        }

        @Override
        public Stream<JetSqlRow> toValues(ExpressionEvalContext context) {
            return values.stream()
                    .flatMap(vs -> ExpressionUtil.evaluate(predicate, projection, vs.toValues(context), context).stream());
        }

        @Override
        public String toString() {
            return "{"
                    + (predicate == null ? "" : "predicate=[" + predicate + "], ")
                    + (projection == null ? "" : "projection=" + projection + ", ")
                    + "values=" + values
                    + "}";
        }
    }
}
