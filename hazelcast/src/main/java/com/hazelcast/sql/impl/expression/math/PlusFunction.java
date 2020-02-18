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
import com.hazelcast.sql.SqlDaySecondInterval;
import com.hazelcast.sql.SqlYearMonthInterval;
import com.hazelcast.sql.impl.expression.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.DataTypeUtils;
import com.hazelcast.sql.impl.type.GenericType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class PlusFunction<T> extends BiCallExpressionWithType<T> {
    public PlusFunction() {
        // No-op.
    }

    private PlusFunction(Expression<?> operand1, Expression<?> operand2, DataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static PlusFunction<?> create(Expression<?> operand1, Expression<?> operand2) {
        DataType operand1Type = operand1.getType();
        DataType operand2Type = operand2.getType();

        DataType resultType = MathFunctionUtils.inferPlusMinusResultType(operand1Type, operand2Type, true);

        if (DataTypeUtils.compare(operand1Type, operand2Type) == operand1Type) {
            return new PlusFunction<>(operand1, operand2, resultType);
        } else {
            return new PlusFunction<>(operand2, operand1, resultType);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        Object operand1Value = operand1.eval(row);

        if (operand1Value == null) {
            return null;
        }

        Object operand2Value = operand2.eval(row);

        if (operand2Value == null) {
            return null;
        }

        return (T) doPlus(operand1Value, operand1.getType(), operand2Value, operand2.getType(), resultType);
    }

    private static Object doPlus(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resultType
    ) {
        // Handle late binding.
        boolean wasLate = resultType.getType() == GenericType.LATE;

        if (wasLate) {
            operand1Type = DataTypeUtils.resolveType(operand1);
            operand2Type = DataTypeUtils.resolveType(operand1);

            resultType = MathFunctionUtils.inferPlusMinusResultType(operand1Type, operand2Type, true);
        }

        // Handle temporal.
        if (resultType.isTemporal()) {
            if (wasLate && DataTypeUtils.compare(operand1Type, operand2Type) == operand2Type) {
                return doPlusTemporal(operand1, operand1Type, operand2, operand2Type, resultType);
            } else {
                return doPlusTemporal(operand2, operand2Type, operand1, operand1Type, resultType);
            }
        }

        // Handle numeric.
        return doPlusNumeric(operand1, operand1Type, operand2, operand2Type, resultType);
    }

    private static Object doPlusNumeric(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resultType
    ) {
        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        switch (resultType.getType()) {
            case TINYINT:
                return operand1Converter.asTinyint(operand1) + operand2Converter.asTinyint(operand2);

            case SMALLINT:
                return operand1Converter.asSmallint(operand1) + operand2Converter.asSmallint(operand2);

            case INT:
                return operand1Converter.asInt(operand1) + operand2Converter.asInt(operand2);

            case BIGINT:
                return operand1Converter.asBigint(operand1) + operand2Converter.asBigint(operand2);

            case DECIMAL:
                BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                return op1Decimal.add(op2Decimal);

            case REAL:
                return operand1Converter.asReal(operand1) + operand2Converter.asReal(operand2);

            case DOUBLE:
                return operand1Converter.asDouble(operand1) + operand2Converter.asDouble(operand2);

            default:
                throw HazelcastSqlException.error("Invalid type: " + resultType);
        }
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object doPlusTemporal(
        Object temporalOperand,
        DataType temporalOperandType,
        Object intervalOperand,
        DataType intervalOperandType,
        DataType resType
    ) {
        switch (resType.getType()) {
            case DATE: {
                LocalDate date = temporalOperandType.getConverter().asDate(temporalOperand);

                if (intervalOperandType.getType() == GenericType.INTERVAL_YEAR_MONTH) {
                    return date.plusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return date.atStartOfDay().plusSeconds(interval.getSeconds()).plusNanos(interval.getNanos()).toLocalDate();
                }
            }

            case TIME: {
                LocalTime time = temporalOperandType.getConverter().asTime(temporalOperand);

                if (intervalOperandType.getType() == GenericType.INTERVAL_YEAR_MONTH) {
                    return time;
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return time.plusSeconds(interval.getSeconds()).plusNanos(interval.getNanos());
                }
            }

            case TIMESTAMP: {
                LocalDateTime ts = temporalOperandType.getConverter().asTimestamp(temporalOperand);

                if (intervalOperandType.getType() == GenericType.INTERVAL_YEAR_MONTH) {
                    return ts.plusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return ts.plusSeconds(interval.getSeconds()).plusNanos(interval.getNanos());
                }
            }

            case TIMESTAMP_WITH_TIMEZONE: {
                OffsetDateTime ts = temporalOperandType.getConverter().asTimestampWithTimezone(temporalOperand);

                if (intervalOperandType.getType() == GenericType.INTERVAL_YEAR_MONTH) {
                    return ts.plusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return ts.plusSeconds(interval.getSeconds()).plusNanos(interval.getNanos());
                }
            }
            default:
                throw HazelcastSqlException.error("Unsupported result type: " + resType);
        }
    }
}
