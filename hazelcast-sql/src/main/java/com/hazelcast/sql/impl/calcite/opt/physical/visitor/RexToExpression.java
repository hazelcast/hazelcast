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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.math.DivideFunction;
import com.hazelcast.sql.impl.expression.math.MinusFunction;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.expression.math.UnaryMinusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.CaseExpression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.NotPredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Calendar;

/**
 * Utility methods for REX to Hazelcast expression conversion.
 */
public final class RexToExpression {

    private RexToExpression() {
        // No-op.
    }

    /**
     * Converts the given REX literal to runtime {@link ConstantExpression
     * constant expression}.
     *
     * @param literal the literal to convert.
     * @return the resulting constant expression.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public static Expression<?> convertLiteral(RexLiteral literal) {
        SqlTypeName type = literal.getType().getSqlTypeName();

        switch (type) {
            case BOOLEAN:
                return convertBooleanLiteral(literal, type);

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case FLOAT:
            case DOUBLE:
                return convertNumericLiteral(literal, type);

            case CHAR:
            case VARCHAR:
                return convertStringLiteral(literal, type);

            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTemporalLiteral(literal, type);

            case NULL:
                return ConstantExpression.create(null, QueryDataType.NULL);

            case ANY:
                // currently, the only possible literal of ANY type is NULL
                assert literal.getValueAs(Object.class) == null;
                return ConstantExpression.create(null, QueryDataType.OBJECT);

            default:
                throw QueryException.error("Unsupported literal: " + literal);
        }
    }

    /**
     * Converts a {@link RexCall} to {@link Expression}.
     *
     * @param call the call to convert.
     * @return the resulting expression.
     * @throws QueryException if the given {@link RexCall} can't be
     *                        converted.
     */
    @SuppressWarnings({"checkstyle:ReturnCount", "checkstyle:CyclomaticComplexity"})
    public static Expression<?> convertCall(RexCall call, Expression<?>[] operands) {
        SqlOperator operator = call.getOperator();
        QueryDataType resultType = SqlToQueryType.map(call.getType().getSqlTypeName());

        switch (operator.getKind()) {
            case CAST:
                return CastExpression.create(operands[0], resultType);

            case CASE:
                return CaseExpression.create(operands, resultType);

            case AND:
                return AndPredicate.create(operands);

            case OR:
                return OrPredicate.create(operands);

            case NOT:
                return NotPredicate.create(operands[0]);

            case PLUS:
                return PlusFunction.create(operands[0], operands[1], resultType);

            case MINUS:
                return MinusFunction.create(operands[0], operands[1], resultType);

            case TIMES:
                return MultiplyFunction.create(operands[0], operands[1], resultType);

            case DIVIDE:
                return DivideFunction.create(operands[0], operands[1], resultType);

            case MINUS_PREFIX:
                return UnaryMinusFunction.create(operands[0], resultType);

            case PLUS_PREFIX:
                return operands[0];

            case EQUALS:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.EQUALS);

            case NOT_EQUALS:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.NOT_EQUALS);

            case GREATER_THAN:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.GREATER_THAN);

            case GREATER_THAN_OR_EQUAL:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.GREATER_THAN_OR_EQUAL);

            case LESS_THAN:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.LESS_THAN);

            case LESS_THAN_OR_EQUAL:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.LESS_THAN_OR_EQUAL);

            case IS_TRUE:
                return IsTruePredicate.create(operands[0]);

            case IS_NOT_TRUE:
                return IsNotTruePredicate.create(operands[0]);

            case IS_FALSE:
                return IsFalsePredicate.create(operands[0]);

            case IS_NOT_FALSE:
                return IsNotFalsePredicate.create(operands[0]);

            case IS_NULL:
                return IsNullPredicate.create(operands[0]);

            case IS_NOT_NULL:
                return IsNotNullPredicate.create(operands[0]);

            default:
                break;
        }

        throw QueryException.error("Unsupported operator: " + operator);
    }

    private static Expression<?> convertBooleanLiteral(RexLiteral literal, SqlTypeName type) {
        assert type == SqlTypeName.BOOLEAN;
        Boolean value = literal.getValueAs(Boolean.class);
        return ConstantExpression.create(value, SqlToQueryType.map(type));
    }

    private static Expression<?> convertNumericLiteral(RexLiteral literal, SqlTypeName type) {
        Object value;
        switch (type) {
            case TINYINT:
                value = literal.getValueAs(Byte.class);
                break;

            case SMALLINT:
                value = literal.getValueAs(Short.class);
                break;

            case INTEGER:
                value = literal.getValueAs(Integer.class);
                break;

            case BIGINT:
                value = literal.getValueAs(Long.class);
                break;

            case DECIMAL:
                value = literal.getValueAs(BigDecimal.class);
                break;

            case REAL:
                value = literal.getValueAs(Float.class);
                break;

            case DOUBLE:
                value = literal.getValueAs(Double.class);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(value, SqlToQueryType.map(type));
    }

    private static Expression<?> convertStringLiteral(RexLiteral literal, SqlTypeName type) {
        Object value;
        switch (type) {
            case CHAR:
            case VARCHAR:
                value = literal.getValueAs(String.class);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(value, SqlToQueryType.map(type));
    }

    private static Expression<?> convertTemporalLiteral(RexLiteral literal, SqlTypeName type) {
        Calendar calendar = literal.getValueAs(Calendar.class);
        CalendarConverter converter = CalendarConverter.INSTANCE;

        Object value;
        switch (type) {
            case DATE:
                value = converter.asDate(calendar);
                break;

            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                value = converter.asTime(calendar);
                break;

            case TIMESTAMP:
                value = converter.asTimestamp(calendar);
                break;

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                value = converter.asTimestampWithTimezone(calendar);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(value, SqlToQueryType.map(type));
    }

}
