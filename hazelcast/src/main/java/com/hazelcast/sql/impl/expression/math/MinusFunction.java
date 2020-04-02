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
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class MinusFunction<T> extends BiExpressionWithType<T> {

    @SuppressWarnings("unused")
    public MinusFunction() {
        // No-op.
    }

    private MinusFunction(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static MinusFunction<?> create(Expression<?> operand1, Expression<?> operand2) {
        QueryDataType resultType = MathFunctionUtils.inferPlusMinusResultType(operand1.getType(), operand2.getType(), false);

        return new MinusFunction<>(operand1, operand2, resultType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        // Calculate child operands with fail-fast NULL semantics.
        Object operand1Value = operand1.eval(row, context);

        if (operand1Value == null) {
            return null;
        }

        Object operand2Value = operand2.eval(row, context);

        if (operand2Value == null) {
            return null;
        }

        return (T) doMinus(operand1Value, operand1.getType(), operand2Value, operand2.getType(), resultType);
    }

    private static Object doMinus(Object operand1, QueryDataType operand1Type, Object operand2, QueryDataType operand2Type,
                                  QueryDataType resultType) {
        if (resultType.getTypeFamily().isTemporal()) {
            return doMinusTemporal(operand1, operand1Type, operand2, operand2Type, resultType);
        }

        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        switch (resultType.getTypeFamily()) {
            case TINYINT:
                return operand1Converter.asTinyint(operand1) - operand2Converter.asTinyint(operand2);

            case SMALLINT:
                return operand1Converter.asSmallint(operand1) - operand2Converter.asSmallint(operand2);

            case INT:
                return operand1Converter.asInt(operand1) - operand2Converter.asInt(operand2);

            case BIGINT:
                return operand1Converter.asBigint(operand1) - operand2Converter.asBigint(operand2);

            case DECIMAL:
                BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                return op1Decimal.subtract(op2Decimal);

            case REAL:
                return operand1Converter.asReal(operand1) - operand2Converter.asReal(operand2);

            case DOUBLE:
                return operand1Converter.asDouble(operand1) - operand2Converter.asDouble(operand2);

            default:
                throw HazelcastSqlException.error("Invalid type: " + resultType);
        }
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object doMinusTemporal(Object temporalOperand, QueryDataType temporalOperandType, Object intervalOperand,
                                          QueryDataType intervalOperandType, QueryDataType resType) {
        switch (resType.getTypeFamily()) {
            case DATE: {
                LocalDate date = temporalOperandType.getConverter().asDate(temporalOperand);

                if (intervalOperandType.getTypeFamily() == QueryDataTypeFamily.INTERVAL_YEAR_MONTH) {
                    return date.minusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return date.atStartOfDay().minusSeconds(interval.getSeconds()).minusNanos(interval.getNanos()).toLocalDate();
                }
            }

            case TIME: {
                LocalTime time = temporalOperandType.getConverter().asTime(temporalOperand);

                if (intervalOperandType.getTypeFamily() == QueryDataTypeFamily.INTERVAL_YEAR_MONTH) {
                    return time;
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return time.minusSeconds(interval.getSeconds()).minusNanos(interval.getNanos());
                }
            }

            case TIMESTAMP: {
                LocalDateTime ts = temporalOperandType.getConverter().asTimestamp(temporalOperand);

                if (intervalOperandType.getTypeFamily() == QueryDataTypeFamily.INTERVAL_YEAR_MONTH) {
                    return ts.minusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return ts.minusSeconds(interval.getSeconds()).minusNanos(interval.getNanos());
                }
            }

            case TIMESTAMP_WITH_TIME_ZONE: {
                OffsetDateTime ts = temporalOperandType.getConverter().asTimestampWithTimezone(temporalOperand);

                if (intervalOperandType.getTypeFamily() == QueryDataTypeFamily.INTERVAL_YEAR_MONTH) {
                    return ts.minusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return ts.minusSeconds(interval.getSeconds()).minusNanos(interval.getNanos());
                }
            }

            default:
                throw HazelcastSqlException.error("Unsupported result type: " + resType);
        }
    }

}
