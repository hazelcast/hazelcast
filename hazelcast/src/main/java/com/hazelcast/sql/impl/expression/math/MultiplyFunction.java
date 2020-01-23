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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlYearMonthInterval;
import com.hazelcast.sql.impl.expression.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;
import com.hazelcast.sql.impl.type.accessor.SqlDaySecondIntervalConverter;
import com.hazelcast.sql.impl.type.accessor.SqlYearMonthIntervalConverter;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.expression.time.TemporalUtils.NANO_IN_SECONDS;
import static com.hazelcast.sql.impl.type.DataType.PRECISION_UNLIMITED;
import static com.hazelcast.sql.impl.type.DataType.SCALE_UNLIMITED;

/**
 * Plus expression.
 */
public class MultiplyFunction<T> extends BiCallExpressionWithType<T> {
    /** Type of the first argument. */
    private transient DataType operand1Type;

    /** Type of the second argument. */
    private transient DataType operand2Type;

    public MultiplyFunction() {
        // No-op.
    }

    public MultiplyFunction(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        // Calculate child operands with fail-fast NULL semantics.
        Object operand1Value = operand1.eval(row);

        if (operand1Value == null) {
            return null;
        }

        Object operand2Value = operand2.eval(row);

        if (operand2Value == null) {
            return null;
        }

        // Prepare result type if needed.
        if (resType == null) {
            DataType type1 = operand1.getType();
            DataType type2 = operand2.getType();

            resType = inferResultType(type1, type2);

            operand1Type = type1;
            operand2Type = type2;
        }

        // Execute.
        return (T) doMultiply(operand1Value, operand1Type, operand2Value, operand2Type, resType);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:AvoidNestedBlocks"})
    private static Object doMultiply(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        switch (resType.getType()) {
            case TINYINT:
                return (byte) (operand1Converter.asTinyInt(operand1) * operand2Converter.asTinyInt(operand2));

            case SMALLINT:
                return (short) (operand1Converter.asSmallInt(operand1) * operand2Converter.asSmallInt(operand2));

            case INT:
                return (operand1Converter.asInt(operand1) * operand2Converter.asInt(operand2));

            case BIGINT:
                return operand1Converter.asBigInt(operand1) * operand2Converter.asBigInt(operand2);

            case DECIMAL:
                BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                return op1Decimal.multiply(op2Decimal);

            case REAL:
                return operand1Converter.asReal(operand1) * operand2Converter.asReal(operand2);

            case DOUBLE:
                return operand1Converter.asDouble(operand1) * operand2Converter.asDouble(operand2);

            case INTERVAL_YEAR_MONTH: {
                SqlYearMonthInterval interval;
                int multiplier;

                if (operand1Converter == SqlYearMonthIntervalConverter.INSTANCE) {
                    interval = (SqlYearMonthInterval) operand1;
                    multiplier = operand2Converter.asInt(operand2);
                } else {
                    interval = (SqlYearMonthInterval) operand2;
                    multiplier = operand1Converter.asInt(operand1);
                }

                return new SqlYearMonthInterval(interval.getMonths() * multiplier);
            }

            case INTERVAL_DAY_SECOND: {
                SqlDaySecondInterval interval;
                long multiplier;

                if (operand1Converter == SqlDaySecondIntervalConverter.INSTANCE) {
                    interval = (SqlDaySecondInterval) operand1;
                    multiplier = operand2Converter.asBigInt(operand2);
                } else {
                    interval = (SqlDaySecondInterval) operand2;
                    multiplier = operand1Converter.asBigInt(operand1);
                }

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
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    @Override public int operator() {
        return CallOperator.MULTIPLY;
    }

    /**
     * Infer result type for multiplication operation.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     * @return Result type.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private static DataType inferResultType(DataType type1, DataType type2) {
        if (type1 == DataType.INTERVAL_DAY_SECOND || type2 == DataType.INTERVAL_DAY_SECOND) {
            return DataType.INTERVAL_DAY_SECOND;
        }

        if (type1 == DataType.INTERVAL_YEAR_MONTH || type2 == DataType.INTERVAL_YEAR_MONTH) {
            return DataType.INTERVAL_YEAR_MONTH;
        }

        if (!type1.isCanConvertToNumeric()) {
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 1 is not numeric.");
        }

        if (!type2.isCanConvertToNumeric()) {
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 2 is not numeric.");
        }

        if (type1 == DataType.VARCHAR) {
            type1 = DataType.DECIMAL;
        }

        if (type2 == DataType.VARCHAR) {
            type2 = DataType.DECIMAL;
        }

        // Precision is expanded to accommodate all numbers: 99 * 99 = 9801;
        int precision = type1.getPrecision() == PRECISION_UNLIMITED || type2.getPrecision() == PRECISION_UNLIMITED
            ? PRECISION_UNLIMITED : type1.getPrecision() + type2.getPrecision();

        int scale = type1.getScale() == SCALE_UNLIMITED || type2.getScale() == SCALE_UNLIMITED ? SCALE_UNLIMITED : 0;

        if (scale == 0) {
            return DataType.integerType(precision);
        } else {
            DataType biggerType = type1.getPrecedence() >= type2.getPrecedence() ? type1 : type2;

            if (biggerType == DataType.REAL) {
                // REAL -> DOUBLE
                return DataType.DOUBLE;
            } else {
                // DECIMAL -> DECIMAL, DOUBLE -> DOUBLE
                return biggerType;
            }
        }
    }
}
