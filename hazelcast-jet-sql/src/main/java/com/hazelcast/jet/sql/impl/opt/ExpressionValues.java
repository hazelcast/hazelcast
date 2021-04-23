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

package com.hazelcast.jet.sql.impl.opt;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpression;
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

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toList;

/**
 * Representation of the VALUES clause data, see subclasses.
 */
public abstract class ExpressionValues implements Serializable {

    public abstract List<Object[]> toValues(ExpressionEvalContext context);

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
        public List<Object[]> toValues(ExpressionEvalContext context) {
            return expressions.stream()
                    .map(es -> es.stream().map(e -> e.eval(EmptyRow.INSTANCE, context)).toArray(Object[]::new))
                    .collect(toList());
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
        public List<Object[]> toValues(ExpressionEvalContext context) {
            return values.stream()
                    .flatMap(vs -> ExpressionUtil.evaluate(predicate, projection, vs.toValues(context), context).stream())
                    .collect(toList());
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
