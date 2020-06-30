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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.BiExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.expression.datetime.DateTimeExpressionUtils.NANO_IN_SECONDS;
import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTERVAL_DAY_SECOND;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTERVAL_YEAR_MONTH;

/**
 * Implements evaluation of SQL multiply operator.
 */
public class MultiplyFunction<T> extends BiExpressionWithType<T> implements IdentifiedDataSerializable {

    public MultiplyFunction() {
        // No-op.
    }

    private MultiplyFunction(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static MultiplyFunction<?> create(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        if (operand2.getType().getTypeFamily() == INTERVAL_DAY_SECOND
                || operand2.getType().getTypeFamily() == INTERVAL_YEAR_MONTH) {
            Expression<?> intervalOperand = operand2;
            operand2 = operand1;
            operand1 = intervalOperand;
        }

        return new MultiplyFunction<>(operand1, operand2, resultType);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_MULTIPLY;
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
            return (T) evalTemporal(operand1, operand2, operand2.getType(), resultType);
        } else {
            return (T) evalNumeric((Number) left, (Number) right, family);
        }
    }

    private static Object evalNumeric(Number left, Number right, QueryDataTypeFamily family) {
        switch (family) {
            case TINYINT:
                return (byte) (left.byteValue() * right.byteValue());
            case SMALLINT:
                return (short) (left.shortValue() * right.shortValue());
            case INT:
                return left.intValue() * right.intValue();
            case BIGINT:
                try {
                    return Math.multiplyExact(left.longValue(), right.longValue());
                } catch (ArithmeticException e) {
                    throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow");
                }
            case REAL:
                return left.floatValue() * right.floatValue();
            case DOUBLE:
                return left.doubleValue() * right.doubleValue();
            case DECIMAL:
                return ((BigDecimal) left).multiply((BigDecimal) right, DECIMAL_MATH_CONTEXT);
            default:
                throw new IllegalArgumentException("unexpected result family: " + family);
        }
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object evalTemporal(Object intervalOperand, Object numericOperand, QueryDataType numericOperandType,
                                       QueryDataType resultType) {
        switch (resultType.getTypeFamily()) {
            case INTERVAL_YEAR_MONTH: {
                SqlYearMonthInterval interval = (SqlYearMonthInterval) intervalOperand;
                int multiplier = numericOperandType.getConverter().asInt(numericOperand);

                return new SqlYearMonthInterval(interval.getMonths() * multiplier);
            }

            case INTERVAL_DAY_SECOND: {
                SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;
                long multiplier = numericOperandType.getConverter().asBigint(numericOperand);

                if (interval.getNanos() == 0) {
                    return new SqlDaySecondInterval(interval.getSeconds() * multiplier, 0);
                } else {
                    long valueMultiplied = interval.getSeconds() * multiplier;
                    long nanosMultiplied = interval.getNanos() * multiplier;

                    long newValue = valueMultiplied + nanosMultiplied / NANO_IN_SECONDS;
                    int newNanos = (int) (nanosMultiplied % NANO_IN_SECONDS);

                    return new SqlDaySecondInterval(newValue, newNanos);
                }
            }

            default:
                throw QueryException.error("Invalid type: " + resultType);
        }
    }

}
