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
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;

import static com.hazelcast.sql.impl.type.DataType.PRECISION_UNLIMITED;
import static com.hazelcast.sql.impl.type.DataType.SCALE_UNLIMITED;

/**
 * Plus expression.
 */
@SuppressWarnings("checkstyle:AvoidNestedBlocks")
public class PlusMinusFunction<T> extends BiCallExpressionWithType<T> {
    /** Minus flag. */
    private boolean minus;

    /** Type of the first argument. */
    private transient DataType operand1Type;

    /** Type of the second argument. */
    private transient DataType operand2Type;

    public PlusMinusFunction() {
        // No-op.
    }

    public PlusMinusFunction(Expression operand1, Expression operand2, boolean minus) {
        super(operand1, operand2);

        this.minus = minus;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        // Calculate child operands with fail-fast NULL semantics.
        Object operand1Value = operand1.eval(ctx, row);

        if (operand1Value == null) {
            return null;
        }

        Object operand2Value = operand2.eval(ctx, row);

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
        if (minus) {
            return (T) doMinus(operand1Value, operand1Type, operand2Value, operand2Type, resType);
        } else {
            return (T) doPlus(operand1Value, operand1Type, operand2Value, operand2Type, resType);
        }
    }

    private static Object doPlus(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        if (resType.isTemporal()) {
            return doPlusTemporal(operand1, operand1Type, operand2, operand2Type, resType);
        }

        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        switch (resType.getType()) {
            case TINYINT:
                return operand1Converter.asTinyInt(operand1) + operand2Converter.asTinyInt(operand2);

            case SMALLINT:
                return operand1Converter.asSmallInt(operand1) + operand2Converter.asSmallInt(operand2);

            case INT:
                return operand1Converter.asInt(operand1) + operand2Converter.asInt(operand2);

            case BIGINT:
                return operand1Converter.asBigInt(operand1) + operand2Converter.asBigInt(operand2);

            case DECIMAL:
                BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                return op1Decimal.add(op2Decimal);

            case REAL:
                return operand1Converter.asReal(operand1) + operand2Converter.asReal(operand2);

            case DOUBLE:
                return operand1Converter.asDouble(operand1) + operand2Converter.asDouble(operand2);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    private static Object doPlusTemporal(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        Object temporalOperand;
        DataType temporalOperandType;
        Object intervalOperand;

        if (operand1Type.isTemporal()) {
            temporalOperand = operand1;
            temporalOperandType = operand1Type;
            intervalOperand = operand2;
        } else {
            temporalOperand = operand2;
            temporalOperandType = operand2Type;
            intervalOperand = operand1;
        }

        switch (resType.getType()) {
            case DATE: {
                LocalDate date = temporalOperandType.getConverter().asDate(temporalOperand);

                if (intervalOperand instanceof SqlYearMonthInterval) {
                    return date.plusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return date.atStartOfDay().plusSeconds(interval.getSeconds()).plusNanos(interval.getNanos()).toLocalDate();
                }
            }

            case TIME: {
                LocalTime time = temporalOperandType.getConverter().asTime(temporalOperand);

                if (intervalOperand instanceof SqlYearMonthInterval) {
                    return time;
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return time.plusSeconds(interval.getSeconds()).plusNanos(interval.getNanos());
                }
            }

            case TIMESTAMP: {
                LocalDateTime ts = temporalOperandType.getConverter().asTimestamp(temporalOperand);

                if (intervalOperand instanceof SqlYearMonthInterval) {
                    return ts.plusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return ts.plusSeconds(interval.getSeconds()).plusNanos(interval.getNanos());
                }
            }

            case TIMESTAMP_WITH_TIMEZONE: {
                OffsetDateTime ts = temporalOperandType.getConverter().asTimestampWithTimezone(temporalOperand);

                if (intervalOperand instanceof SqlYearMonthInterval) {
                    return ts.plusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return ts.plusSeconds(interval.getSeconds()).plusNanos(interval.getNanos());
                }
            }
            default:
                throw new HazelcastSqlException(-1, "Unsupported result type: " + resType);
        }
    }

    private static Object doMinus(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        if (resType.isTemporal()) {
            return doMinusTemporal(operand1, operand1Type, operand2, operand2Type, resType);
        }

        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        switch (resType.getType()) {
            case TINYINT:
                return operand1Converter.asTinyInt(operand1) - operand2Converter.asTinyInt(operand2);

            case SMALLINT:
                return operand1Converter.asSmallInt(operand1) - operand2Converter.asSmallInt(operand2);

            case INT:
                return operand1Converter.asInt(operand1) - operand2Converter.asInt(operand2);

            case BIGINT:
                return operand1Converter.asBigInt(operand1) - operand2Converter.asBigInt(operand2);

            case DECIMAL:
                BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                return op1Decimal.subtract(op2Decimal);

            case REAL:
                return operand1Converter.asReal(operand1) - operand2Converter.asReal(operand2);

            case DOUBLE:
                return operand1Converter.asDouble(operand1) - operand2Converter.asDouble(operand2);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    private static Object doMinusTemporal(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        Object temporalOperand;
        DataType temporalOperandType;
        Object intervalOperand;

        if (operand1Type.isTemporal()) {
            temporalOperand = operand1;
            temporalOperandType = operand1Type;
            intervalOperand = operand2;
        } else {
            temporalOperand = operand2;
            temporalOperandType = operand2Type;
            intervalOperand = operand1;
        }

        switch (resType.getType()) {
            case DATE: {
                LocalDate date = temporalOperandType.getConverter().asDate(temporalOperand);

                if (intervalOperand instanceof SqlYearMonthInterval) {
                    return date.minusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return date.atStartOfDay().minusSeconds(interval.getSeconds()).minusNanos(interval.getNanos()).toLocalDate();
                }
            }

            case TIME: {
                LocalTime time = temporalOperandType.getConverter().asTime(temporalOperand);

                if (intervalOperand instanceof SqlYearMonthInterval) {
                    return time;
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return time.minusSeconds(interval.getSeconds()).minusNanos(interval.getNanos());
                }
            }

            case TIMESTAMP: {
                LocalDateTime ts = temporalOperandType.getConverter().asTimestamp(temporalOperand);

                if (intervalOperand instanceof SqlYearMonthInterval) {
                    return ts.minusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return ts.minusSeconds(interval.getSeconds()).minusNanos(interval.getNanos());
                }
            }

            case TIMESTAMP_WITH_TIMEZONE: {
                OffsetDateTime ts = temporalOperandType.getConverter().asTimestampWithTimezone(temporalOperand);

                if (intervalOperand instanceof SqlYearMonthInterval) {
                    return ts.minusDays(((SqlYearMonthInterval) intervalOperand).getMonths());
                } else {
                    SqlDaySecondInterval interval = (SqlDaySecondInterval) intervalOperand;

                    return ts.minusSeconds(interval.getSeconds()).minusNanos(interval.getNanos());
                }
            }

            default:
                throw new HazelcastSqlException(-1, "Unsupported result type: " + resType);
        }
    }

    /**
     * Infer result type.
     *
     * @param type1 Type of the first operand.
     * @param type2 Type of the second operand.
     * @return Result type.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private static DataType inferResultType(DataType type1, DataType type2) {
        if (type1.isTemporal() || type2.isTemporal()) {
            return inferResultTypeTemporal(type1, type2);
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

        // Precision is expanded by 1 to handle overflow: 9 + 1 = 10
        int precision = type1.getPrecision() == PRECISION_UNLIMITED || type2.getPrecision() == PRECISION_UNLIMITED
            ? PRECISION_UNLIMITED : Math.max(type1.getPrecision(), type2.getPrecision()) + 1;

        // We have only unlimited or zero scales.
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

    /**
     * Infer result type for temporal arguments.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     * @return Result type.
     */
    private static DataType inferResultTypeTemporal(DataType type1, DataType type2) {
        DataType temporalType = type1.isTemporal() ? type1 : type2;
        DataType intervalType = type1.isTemporal() ? type2 : type1;

        if (intervalType != DataType.INTERVAL_DAY_SECOND && intervalType != DataType.INTERVAL_YEAR_MONTH) {
            throw new HazelcastSqlException(-1, "Data type is not interval: " + intervalType);
        }

        return temporalType;
    }

    @Override public int operator() {
        return minus ? CallOperator.MINUS : CallOperator.PLUS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(minus);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        minus = in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof PlusMinusFunction)) {
            return false;
        }

        PlusMinusFunction<?> that = (PlusMinusFunction<?>) o;

        return minus == that.minus && Objects.equals(operand1, that.operand1) && Objects.equals(operand2, that.operand2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minus, operand1, operand2);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{minus=" + minus + ", operand1=" + operand1 + ", operand2=" + operand2 + '}';
    }
}
