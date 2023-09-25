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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.BiExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;

import java.math.BigDecimal;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.canSimplifyTemporalPlusMinus;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.DECIMAL_MATH_CONTEXT;

/**
 * Implements evaluation of SQL minus operator.
 */
public final class MinusFunction<T> extends BiExpressionWithType<T> {

    public MinusFunction() {
        // No-op.
    }

    private MinusFunction(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static Expression<?> create(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        if (canSimplifyTemporalPlusMinus(operand1, operand2)) {
            return operand1;
        }

        return new MinusFunction<>(operand1, operand2, resultType);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_MINUS;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object left = operand1.eval(row, context);
        if (left == null) {
            return null;
        }

        Object right = operand2.eval(row, context);
        if (right == null) {
            return null;
        }

        QueryDataTypeFamily family = resultType.getTypeFamily();

        if (family.isTemporal()) {
            return (T) evalTemporal(left, right, family);
        }

        return (T) evalNumeric((Number) left, (Number) right, family);
    }

    private static Object evalNumeric(Number left, Number right, QueryDataTypeFamily family) {
        switch (family) {
            case TINYINT:
                return (byte) (left.byteValue() - right.byteValue());
            case SMALLINT:
                return (short) (left.shortValue() - right.shortValue());
            case INTEGER:
                return left.intValue() - right.intValue();
            case BIGINT:
                try {
                    return Math.subtractExact(left.longValue(), right.longValue());
                } catch (ArithmeticException e) {
                    throw QueryException.error(SqlErrorCode.DATA_EXCEPTION,
                            "BIGINT overflow in '-' operator (consider adding explicit CAST to DECIMAL)");
                }
            case REAL:
                return left.floatValue() - right.floatValue();
            case DOUBLE:
                return left.doubleValue() - right.doubleValue();
            case DECIMAL:
                return ((BigDecimal) left).subtract((BigDecimal) right, DECIMAL_MATH_CONTEXT);
            default:
                throw new IllegalArgumentException("unexpected result family: " + family);
        }
    }

    private static Object evalTemporal(Object left, Object right, QueryDataTypeFamily family) {
        switch (family) {
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
                Temporal temporal = (Temporal) left;

                if (right instanceof SqlDaySecondInterval) {
                    return temporal.minus(((SqlDaySecondInterval) right).getMillis(), ChronoUnit.MILLIS);
                } else {
                    assert family != TIME;
                    assert right instanceof SqlYearMonthInterval;

                    return temporal.minus(((SqlYearMonthInterval) right).getMonths(), ChronoUnit.MONTHS);
                }

            default:
                throw new IllegalArgumentException("Unexpected result family: " + family);
        }
    }
}
