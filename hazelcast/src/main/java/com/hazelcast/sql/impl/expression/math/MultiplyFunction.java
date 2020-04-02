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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.BiExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.expression.datetime.DateTimeExpressionUtils.NANO_IN_SECONDS;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTERVAL_DAY_SECOND;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTERVAL_YEAR_MONTH;

/**
 * Plus expression.
 */
public class MultiplyFunction<T> extends BiExpressionWithType<T> {

    @SuppressWarnings("unused")
    public MultiplyFunction() {
        // No-op.
    }

    private MultiplyFunction(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static MultiplyFunction<?> create(Expression<?> operand1, Expression<?> operand2) {
        QueryDataType operand1Type = operand1.getType();
        QueryDataType operand2Type = operand2.getType();

        QueryDataType resultType = MathFunctionUtils.inferMultiplyResultType(operand1Type, operand2Type);

        if (QueryDataTypeUtils.withHigherPrecedence(operand1Type, operand2Type) == operand1Type) {
            return new MultiplyFunction<>(operand1, operand2, resultType);
        } else {
            return new MultiplyFunction<>(operand2, operand1, resultType);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object operand1Value = operand1.eval(row, context);

        if (operand1Value == null) {
            return null;
        }

        Object operand2Value = operand2.eval(row, context);

        if (operand2Value == null) {
            return null;
        }

        // Execute.
        return (T) doMultiply(operand1Value, operand1.getType(), operand2Value, operand2.getType(), resultType);
    }

    private static Object doMultiply(Object operand1, QueryDataType operand1Type, Object operand2, QueryDataType operand2Type,
                                     QueryDataType resultType) {
        if (resultType.getTypeFamily() == INTERVAL_YEAR_MONTH || resultType.getTypeFamily() == INTERVAL_DAY_SECOND) {
            return doMultiplyInterval(operand1, operand2, operand2Type, resultType);
        }

        return doMultiplyNumeric(operand1, operand1Type, operand2, operand2Type, resultType);
    }

    private static Object doMultiplyNumeric(Object operand1, QueryDataType operand1Type, Object operand2,
                                            QueryDataType operand2Type, QueryDataType resultType) {
        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        switch (resultType.getTypeFamily()) {
            case TINYINT:
                return (byte) (operand1Converter.asTinyint(operand1) * operand2Converter.asTinyint(operand2));

            case SMALLINT:
                return (short) (operand1Converter.asSmallint(operand1) * operand2Converter.asSmallint(operand2));

            case INT:
                return (operand1Converter.asInt(operand1) * operand2Converter.asInt(operand2));

            case BIGINT:
                return operand1Converter.asBigint(operand1) * operand2Converter.asBigint(operand2);

            case DECIMAL:
                BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                return op1Decimal.multiply(op2Decimal);

            case REAL:
                return operand1Converter.asReal(operand1) * operand2Converter.asReal(operand2);

            case DOUBLE:
                return operand1Converter.asDouble(operand1) * operand2Converter.asDouble(operand2);

            default:
                throw HazelcastSqlException.error("Invalid type: " + resultType);
        }
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object doMultiplyInterval(Object intervalOperand, Object numericOperand, QueryDataType numericOperandType,
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
                throw HazelcastSqlException.error("Invalid type: " + resultType);
        }
    }

}
