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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlDaySecondInterval;
import com.hazelcast.sql.SqlYearMonthInterval;
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

/**
 * Comparison predicates: {@code =}, {@code <>}, {@code <}, {@code <=}, {@code >}, {@code >=}.
 */
public class ComparisonPredicate extends BiCallExpressionWithType<Boolean> {
    /** Operator. */
    private int operator;

    /** Type of the first operand. */
    private transient DataType operand1Type;

    /** Type of the second operand. */
    private transient DataType operand2Type;

    public ComparisonPredicate() {
        // No-op.
    }

    public ComparisonPredicate(Expression operand1, Expression operand2, int operator) {
        super(operand1, operand2);

        this.operator = operator;
    }

    @Override
    public Boolean eval(Row row) {
        Object operand1Value = operand1.eval(row);

        if (operand1Value == null) {
            return null;
        }

        Object operand2Value = operand2.eval(row);

        if (operand2Value == null) {
            return null;
        }

        if (resType == null) {
            DataType type1 = operand1.getType();
            DataType type2 = operand2.getType();

            resType = inferResultType(type1, type2);

            operand1Type = type1;
            operand2Type = type2;
        }

        return doCompare(operator, operand1Value, operand1Type, operand2Value, operand2Type, resType);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:AvoidNestedBlocks"})
    private static boolean doCompare(
        int operator,
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        Converter converter1 = operand1Type.getConverter();
        Converter converter2 = operand2Type.getConverter();

        switch (resType.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT: {
                long first = converter1.asBigInt(operand1);
                long second = converter2.asBigInt(operand2);

                return doCompareLong(operator, first, second);
            }

            case DECIMAL: {
                BigDecimal first = converter1.asDecimal(operand1);
                BigDecimal second = converter2.asDecimal(operand2);

                return doCompareComparable(operator, first, second);
            }

            case REAL:
            case DOUBLE: {
                double first = converter1.asDouble(operand1);
                double second = converter2.asDouble(operand2);

                return doCompareDouble(operator, first, second);
            }

            case DATE: {
                LocalDate first = converter1.asDate(operand1);
                LocalDate second = converter2.asDate(operand2);

                return doCompareComparable(operator, first, second);
            }

            case TIME: {
                LocalTime first = converter1.asTime(operand1);
                LocalTime second = converter2.asTime(operand2);

                return doCompareComparable(operator, first, second);
            }

            case TIMESTAMP: {
                LocalDateTime first = converter1.asTimestamp(operand1);
                LocalDateTime second = converter2.asTimestamp(operand2);

                return doCompareComparable(operator, first, second);
            }

            case TIMESTAMP_WITH_TIMEZONE: {
                OffsetDateTime first = converter1.asTimestampWithTimezone(operand1);
                OffsetDateTime second = converter2.asTimestampWithTimezone(operand2);

                return doCompareComparable(operator, first, second);
            }

            case INTERVAL_DAY_SECOND:
                return doCompareComparable(operator, (SqlDaySecondInterval) operand1, (SqlDaySecondInterval) operand2);

            case INTERVAL_YEAR_MONTH:
                return doCompareComparable(operator, (SqlYearMonthInterval) operand1, (SqlYearMonthInterval) operand2);

            case VARCHAR: {
                String first = converter1.asVarchar(operand1);
                String second = converter2.asVarchar(operand2);

                return doCompareComparable(operator, first, second);
            }

            default:
                throw new HazelcastSqlException(-1, "Unsupported result type: " + resType);
        }
    }

    private static boolean doCompareLong(int operator, long first, long second) {
        int compare = Long.compare(first, second);

        return doCompare0(operator, compare);
    }

    private static boolean doCompareDouble(int operator, double first, double second) {
        int compare = Double.compare(first, second);

        return doCompare0(operator, compare);
    }

    @SuppressWarnings("unchecked")
    private static boolean doCompareComparable(int operator, Comparable first, Comparable second) {
        int compare = first.compareTo(second);

        return doCompare0(operator, compare);
    }

    private static boolean doCompare0(int operator, int compare) {
        switch (operator) {
            case CallOperator.EQUALS:
                return compare == 0;

            case CallOperator.NOT_EQUALS:
                return compare != 0;

            case CallOperator.GREATER_THAN:
                return compare > 0;

            case CallOperator.GREATER_THAN_EQUAL:
                return compare >= 0;

            case CallOperator.LESS_THAN:
                return compare < 0;

            case CallOperator.LESS_THAN_EQUAL:
                return compare <= 0;

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    /**
     * Infer result type. Types must be compatible with each other for this to work.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     * @return Result type.
     */
    private DataType inferResultType(DataType type1, DataType type2) {
        if (type1.getType() == type2.getType()) {
            return type1;
        }

        DataType biggerType;
        DataType smallerType;

        if (type1.getPrecedence() > type2.getPrecedence()) {
            biggerType = type1;
            smallerType = type2;
        } else {
            biggerType = type2;
            smallerType = type1;
        }

        checkTypeCompatibility(biggerType, smallerType);

        return biggerType;
    }

    /**
     * Check if two types are compatible.
     *
     * @param biggerType Bigger type.
     * @param smallerType Smaller type.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void checkTypeCompatibility(DataType biggerType, DataType smallerType) {
        switch (biggerType.getType()) {
            case TIMESTAMP_WITH_TIMEZONE:
            case TIMESTAMP:
                // Any smaller temporal type could be converted to a timestamp, hence check for temporal trait.
                if (!smallerType.isTemporal()) {
                    throw new HazelcastSqlException(-1, "Type cannot be converted to " + biggerType + ": "
                        + smallerType);
                }

                break;

            case DATE:
                // DATE cannot be converted to TIME, do only strict type check.
                checkTypeEqual(biggerType, smallerType);

                break;

            case TIME:
                // DATE is the smallest type of all temporal types, do only strict type check.
                checkTypeEqual(biggerType, smallerType);

                break;

            case INTERVAL_DAY_SECOND:
            case INTERVAL_YEAR_MONTH:
                checkTypeEqual(biggerType, smallerType);

                break;

            case DOUBLE:
            case REAL:
            case DECIMAL:
            case BIGINT:
            case INT:
            case SMALLINT:
            case TINYINT:
            case BIT:
                if (!smallerType.isCanConvertToNumeric()) {
                    throw new HazelcastSqlException(-1, "Type cannot be converted to " + biggerType + ": "
                        + smallerType);
                }

                break;

            case VARCHAR:
                checkTypeEqual(biggerType, smallerType);

                break;

            default:
                throw new HazelcastSqlException(-1, "Unsupported type: " + biggerType);
        }
    }

    private void checkTypeEqual(DataType expType, DataType actualType) {
        if (expType != actualType) {
            throw new HazelcastSqlException(-1, "Type cannot be converted to " + expType + ": " + actualType);
        }
    }

    @Override
    public DataType getType() {
        return DataType.BIT;
    }

    @Override
    public int operator() {
        return operator;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(operator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        operator = in.readInt();
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, operand1, operand2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ComparisonPredicate that = (ComparisonPredicate) o;

        return operator == that.operator && operand1.equals(that.operand1) && operand2.equals(that.operand2);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operator=" + operator + ", operand1=" + operand1 + ", operand2=" + operand2 + '}';
    }
}
