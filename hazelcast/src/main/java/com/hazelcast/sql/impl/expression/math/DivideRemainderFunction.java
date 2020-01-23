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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.hazelcast.sql.impl.expression.time.TemporalUtils.NANO_IN_SECONDS;

/**
 * Divide and remainder functions.
 */
public class DivideRemainderFunction<T> extends BiCallExpressionWithType<T> {
    /** Whether this is a remainder function. */
    private boolean remainder;

    /** Type of the first argument. */
    private transient DataType operand1Type;

    /** Type of the second argument. */
    private transient DataType operand2Type;

    public DivideRemainderFunction() {
        // No-op.
    }

    public DivideRemainderFunction(Expression operand1, Expression operand2, boolean remainder) {
        super(operand1, operand2);

        this.remainder = remainder;
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
        if (remainder) {
            return (T) doRemainder(operand1Value, operand1Type, operand2Value, operand2Type, resType);
        } else {
            return (T) doDivide(operand1Value, operand1Type, operand2Value, operand2Type, resType);
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength",
        "checkstyle:ReturnCount", "checkstyle:AvoidNestedBlocks"})
    private static Object doDivide(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        try {
            switch (resType.getType()) {
                case TINYINT:
                    return (byte) (operand1Converter.asTinyInt(operand1) / operand2Converter.asTinyInt(operand2));

                case SMALLINT:
                    return (short) (operand1Converter.asSmallInt(operand1) / operand2Converter.asSmallInt(operand2));

                case INT:
                    return operand1Converter.asInt(operand1) / operand2Converter.asInt(operand2);

                case BIGINT:
                    return operand1Converter.asBigInt(operand1) / operand2Converter.asBigInt(operand2);

                case DECIMAL:
                    BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                    BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                    return op1Decimal.divide(op2Decimal, DataType.SCALE_DIVIDE, RoundingMode.HALF_DOWN);

                case REAL: {
                    float res = operand1Converter.asReal(operand1) / operand2Converter.asReal(operand2);

                    if (Float.isInfinite(res)) {
                        throw new HazelcastSqlException(-1, "Division by zero.");
                    }

                    return res;
                }

                case DOUBLE: {
                    double res = operand1Converter.asDouble(operand1) / operand2Converter.asDouble(operand2);

                    if (Double.isInfinite(res)) {
                        throw new HazelcastSqlException(-1, "Division by zero.");
                    }

                    return res;
                }

                case INTERVAL_YEAR_MONTH: {
                    SqlYearMonthInterval interval;
                    int divisor;

                    if (operand1Converter == SqlYearMonthIntervalConverter.INSTANCE) {
                        interval = (SqlYearMonthInterval) operand1;
                        divisor = operand2Converter.asInt(operand2);
                    } else {
                        interval = (SqlYearMonthInterval) operand2;
                        divisor = operand1Converter.asInt(operand1);
                    }

                    return new SqlYearMonthInterval(interval.getMonths() / divisor);
                }

                case INTERVAL_DAY_SECOND: {
                    SqlDaySecondInterval interval;
                    long divisor;

                    if (operand1Converter == SqlDaySecondIntervalConverter.INSTANCE) {
                        interval = (SqlDaySecondInterval) operand1;
                        divisor = operand2Converter.asBigInt(operand2);
                    } else {
                        interval = (SqlDaySecondInterval) operand2;
                        divisor = operand1Converter.asBigInt(operand1);
                    }

                    long totalNanos = (interval.getSeconds() * NANO_IN_SECONDS + interval.getNanos()) / divisor;

                    long newValue = totalNanos / NANO_IN_SECONDS;
                    int newNanos = (int) (totalNanos % NANO_IN_SECONDS);

                    return new SqlDaySecondInterval(newValue, newNanos);
                }

                default:
                    throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
            }
        } catch (ArithmeticException e) {
            throw new HazelcastSqlException(-1, "Division by zero.");
        }
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object doRemainder(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        Converter accessor1 = operand1Type.getConverter();
        Converter accessor2 = operand2Type.getConverter();

        try {
            switch (resType.getType()) {
                case TINYINT:
                    return (byte) (accessor1.asTinyInt(operand1) % accessor2.asTinyInt(operand2));

                case SMALLINT:
                    return (short) (accessor1.asSmallInt(operand1) % accessor2.asSmallInt(operand2));

                case INT:
                    return accessor1.asInt(operand1) % accessor2.asInt(operand2);

                case BIGINT:
                    return accessor1.asBigInt(operand1) % accessor2.asBigInt(operand2);

                case DECIMAL:
                    BigDecimal op1Decimal = accessor1.asDecimal(operand1);
                    BigDecimal op2Decimal = accessor2.asDecimal(operand2);

                    return op1Decimal.remainder(op2Decimal);

                case REAL: {
                    float res = accessor1.asReal(operand1) % accessor2.asReal(operand2);

                    if (Float.isInfinite(res)) {
                        throw new HazelcastSqlException(-1, "Division by zero.");
                    }

                    return res;
                }

                case DOUBLE: {
                    double res = accessor1.asDouble(operand1) % accessor2.asDouble(operand2);

                    if (Double.isInfinite(res)) {
                        throw new HazelcastSqlException(-1, "Division by zero.");
                    }

                    return res;
                }

                default:
                    throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
            }
        } catch (ArithmeticException e) {
            throw new HazelcastSqlException(-1, "Division by zero.");
        }
    }

    /**
     * Infer result type.
     *
     * @param type1 Type of the first operand.
     * @param type2 Type of the second operand.
     * @return Result type.
     */
    @SuppressWarnings("checkstyle:NPathComplexity")
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

        DataType higherType = type1.getPrecedence() > type2.getPrecedence() ? type1 : type2;

        if (higherType == DataType.BIT) {
            higherType = DataType.TINYINT;
        }

        return higherType;
    }

    @Override public int operator() {
        return remainder ? CallOperator.REMAINDER : CallOperator.DIVIDE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(remainder);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        remainder = in.readBoolean();
    }
}
