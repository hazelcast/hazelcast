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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import com.hazelcast.sql.impl.expression.string.ConcatFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExpressionTranslator {

    private static final Set<Class<?>> SUPPORTED_EXPRESSIONS;

    static {
        SUPPORTED_EXPRESSIONS = new HashSet<>();
        SUPPORTED_EXPRESSIONS.add(ComparisonPredicate.class);
        SUPPORTED_EXPRESSIONS.add(OrPredicate.class);
        SUPPORTED_EXPRESSIONS.add(AndPredicate.class);
    }

    private final ExpressionEvalContext evalContext;
    private final List<String> fields;

    public ExpressionTranslator(ExpressionEvalContext evalContext, List<String> fields) {
        this.evalContext = evalContext;
        this.fields = fields;
    }

    static void validateExpression(Expression<Boolean> expression) {
        if (expression == null) {
            return;
        }

        if (!SUPPORTED_EXPRESSIONS.contains(expression.getClass())) {
            throw new IllegalArgumentException("Unsupported predicate " + expression);
        }
    }

    @SuppressWarnings("ReturnCount")
    public String translate(Expression<?> expression0) {

        // TODO this grew a bit more than expected, introduce e.g. visitor?
        // or maybe the functionality already exist in Calcite
        if (expression0 instanceof ComparisonPredicate) {
            ComparisonPredicate predicate = (ComparisonPredicate) expression0;
            return translate(predicate.operand1()) + translate(predicate.mode()) + translate(predicate.operand2());
        } else if (expression0 instanceof OrPredicate) {
            OrPredicate orPredicate = (OrPredicate) expression0;
            StringBuilder sb = new StringBuilder();
            Expression<?>[] operands = orPredicate.operands();
            for (int i = 0; i < operands.length; i++) {
                sb.append(translate(operands[i]));
                if (i < (operands.length - 1)) {
                    sb.append(" OR ");
                }
            }
            return sb.toString();
        } else if (expression0 instanceof AndPredicate) {
            AndPredicate andPredicate = (AndPredicate) expression0;
            StringBuilder sb = new StringBuilder();
            Expression<?>[] operands = andPredicate.operands();
            for (int i = 0; i < operands.length; i++) {
                sb.append(translate(operands[i]));
                if (i < (operands.length - 1)) {
                    sb.append(" AND ");
                }
            }
            return sb.toString();
        } else if (expression0 instanceof ColumnExpression) {
            ColumnExpression<?> expression = (ColumnExpression<?>) expression0;
            return fields.get(expression.index());
        } else if (expression0 instanceof ParameterExpression) {
            ParameterExpression<?> expression = (ParameterExpression<?>) expression0;
            return evalContext.getArgument(expression.index()).toString();
        } else if (expression0 instanceof ConcatFunction) {
            ConcatFunction concatFunction = (ConcatFunction) expression0;
            return translate(concatFunction.operand1()) + "||" + translate(concatFunction.operand2());
        } else if (expression0 instanceof CastExpression) {
            CastExpression castExpression = (CastExpression) expression0;
            return translate(castExpression.getOperand());
        } else if (expression0 instanceof ConstantExpression) {
            ConstantExpression constantExpression = (ConstantExpression) expression0;

            // TODO this doesn't work for all types
            if (constantExpression.getType().getTypeFamily().isNumeric()) {
                return constantExpression.getValue().toString();
            } else {
                return '\'' + constantExpression.getValue().toString() + '\'';
            }
        } else {
            throw new IllegalStateException("Unsupported type of expression " + expression0);
        }
    }

    private String translate(ComparisonMode mode) {
        // TODO implement and test all comparisons
        switch (mode) {
            case EQUALS:
                return "=";

            default:
                throw new IllegalStateException("Unsupported comparison mode " + mode);
        }
    }
}
