/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
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

/**
 * Implements evaluation of SQL plus operator.
 */
public final class PlusFunction<T> extends BiExpressionWithType<T> implements IdentifiedDataSerializable {

    public PlusFunction() {
        // No-op.
    }

    private PlusFunction(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static Expression<?> create(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        if (canSimplifyTemporalPlusMinus(operand1, operand2)) {
            return operand1;
        }

        if (canSimplifyTemporalPlusMinus(operand2, operand1)) {
            return operand2;
        }

        if (operand2.getType().getTypeFamily().isTemporal()) {
            // Ensure that interval is always on the right, to simplify the evaluation.
            return new PlusFunction<>(operand2, operand1, resultType);
        } else {
            return new PlusFunction<>(operand1, operand2, resultType);
        }
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_PLUS;
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
                return (byte) (left.byteValue() + right.byteValue());
            case SMALLINT:
                return (short) (left.shortValue() + right.shortValue());
            case INTEGER:
                return left.intValue() + right.intValue();
            case BIGINT:
                try {
                    return Math.addExact(left.longValue(), right.longValue());
                } catch (ArithmeticException e) {
                    throw QueryException.error(SqlErrorCode.DATA_EXCEPTION,
                            "BIGINT overflow in '+' operator (consider adding explicit CAST to DECIMAL)");
                }
            case REAL:
                return left.floatValue() + right.floatValue();
            case DOUBLE:
                return left.doubleValue() + right.doubleValue();
            case DECIMAL:
                return ((BigDecimal) left).add((BigDecimal) right, ExpressionMath.DECIMAL_MATH_CONTEXT);
            default:
                throw new IllegalArgumentException("Unexpected result family: " + family);
        }
    }

    private static Object evalTemporal(Object left, Object right, QueryDataTypeFamily family) {
        switch (family) {
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
                Temporal temporal = (Temporal) left;

                if (right instanceof SqlDaySecondInterval) {
                    return temporal.plus(((SqlDaySecondInterval) right).getMillis(), ChronoUnit.MILLIS);
                } else {
                    assert family != TIME;
                    assert right instanceof SqlYearMonthInterval;

                    return temporal.plus(((SqlYearMonthInterval) right).getMonths(), ChronoUnit.MONTHS);
                }

            default:
                throw new IllegalArgumentException("Unexpected result family: " + family);
        }
    }
}
