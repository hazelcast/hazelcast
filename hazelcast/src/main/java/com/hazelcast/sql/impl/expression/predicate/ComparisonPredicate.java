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
import com.hazelcast.sql.impl.expression.BiCallExpression;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.DataTypeUtils;
import com.hazelcast.sql.impl.type.GenericType;
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
public class ComparisonPredicate extends BiCallExpression<Boolean> {
    /** Data type which is used for comparison. */
    private DataType type;

    /** Operator. */
    private ComparisonMode comparisonMode;

    public ComparisonPredicate() {
        // No-op.
    }

    private ComparisonPredicate(Expression<?> first, Expression<?> second, DataType type, ComparisonMode comparisonMode) {
        super(first, second);

        this.type = type;
        this.comparisonMode = comparisonMode;
    }

    public static ComparisonPredicate create(Expression<?> first, Expression<?> second, ComparisonMode comparisonMode) {
        DataType type = DataTypeUtils.compare(first.getType(), second.getType());

        Expression<?> coercedFirst = CastExpression.coerce(first, type);
        Expression<?> coercedSecond = CastExpression.coerce(second, type);

        return new ComparisonPredicate(coercedFirst, coercedSecond, type, comparisonMode);
    }

    @Override
    public Boolean eval(Row row) {
        Object operand1Value = operand1.eval(row);
        Object operand2Value = operand2.eval(row);

        if (operand1Value == null) {
            return null;
        }

        if (operand2Value == null) {
            return null;
        }

        return doCompare(comparisonMode, operand1Value, operand1.getType(), operand2Value, operand2.getType(), type);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount",
        "checkstyle:AvoidNestedBlocks"})
    private static boolean doCompare(
        ComparisonMode comparisonMode,
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType type
    ) {
        Converter converter1 = operand1Type.getConverter();
        Converter converter2 = operand2Type.getConverter();

        if (type.getType() == GenericType.LATE) {
            // Handle special case when we couldn't resolve the type in advance.
            operand1Type = DataTypeUtils.resolveType(operand1);
            operand2Type = DataTypeUtils.resolveType(operand2);

            type = DataTypeUtils.compare(operand1Type, operand2Type);

            operand1 = CastExpression.coerce(operand1, operand1Type, type);
            operand2 = CastExpression.coerce(operand2, operand2Type, type);
        }

        switch (type.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT: {
                long first = converter1.asBigint(operand1);
                long second = converter2.asBigint(operand2);

                return doCompareLong(comparisonMode, first, second);
            }

            case DECIMAL: {
                BigDecimal first = converter1.asDecimal(operand1);
                BigDecimal second = converter2.asDecimal(operand2);

                return doCompareComparable(comparisonMode, first, second);
            }

            case REAL:
            case DOUBLE: {
                double first = converter1.asDouble(operand1);
                double second = converter2.asDouble(operand2);

                return doCompareDouble(comparisonMode, first, second);
            }

            case DATE: {
                LocalDate first = converter1.asDate(operand1);
                LocalDate second = converter2.asDate(operand2);

                return doCompareComparable(comparisonMode, first, second);
            }

            case TIME: {
                LocalTime first = converter1.asTime(operand1);
                LocalTime second = converter2.asTime(operand2);

                return doCompareComparable(comparisonMode, first, second);
            }

            case TIMESTAMP: {
                LocalDateTime first = converter1.asTimestamp(operand1);
                LocalDateTime second = converter2.asTimestamp(operand2);

                return doCompareComparable(comparisonMode, first, second);
            }

            case TIMESTAMP_WITH_TIMEZONE: {
                OffsetDateTime first = converter1.asTimestampWithTimezone(operand1);
                OffsetDateTime second = converter2.asTimestampWithTimezone(operand2);

                return doCompareComparable(comparisonMode, first, second);
            }

            case INTERVAL_DAY_SECOND:
                return doCompareComparable(comparisonMode, (SqlDaySecondInterval) operand1, (SqlDaySecondInterval) operand2);

            case INTERVAL_YEAR_MONTH:
                return doCompareComparable(comparisonMode, (SqlYearMonthInterval) operand1, (SqlYearMonthInterval) operand2);

            case VARCHAR: {
                String first = converter1.asVarchar(operand1);
                String second = converter2.asVarchar(operand2);

                return doCompareComparable(comparisonMode, first, second);
            }

            default:
                throw HazelcastSqlException.error("Unsupported result type: " + type);
        }
    }

    private static boolean doCompareLong(ComparisonMode comparisonMode, long first, long second) {
        int compare = Long.compare(first, second);

        return doCompare0(comparisonMode, compare);
    }

    private static boolean doCompareDouble(ComparisonMode comparisonMode, double first, double second) {
        int compare = Double.compare(first, second);

        return doCompare0(comparisonMode, compare);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean doCompareComparable(ComparisonMode comparisonMode, Comparable first, Comparable second) {
        int compare = first.compareTo(second);

        return doCompare0(comparisonMode, compare);
    }

    private static boolean doCompare0(ComparisonMode type, int compare) {
        switch (type) {
            case EQUALS:
                return compare == 0;

            case NOT_EQUALS:
                return compare != 0;

            case GREATER_THAN:
                return compare > 0;

            case GREATER_THAN_EQUAL:
                return compare >= 0;

            case LESS_THAN:
                return compare < 0;

            case LESS_THAN_EQUAL:
                return compare <= 0;

            default:
                throw HazelcastSqlException.error("Unsupported operator: " + type);
        }
    }

    @Override
    public DataType getType() {
        return DataType.BIT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeObject(type);
        out.writeInt(comparisonMode.getId());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        type = in.readObject();
        comparisonMode = ComparisonMode.getById(in.readInt());
    }

    @Override
    public int hashCode() {
        return Objects.hash(operand1, operand2, type, comparisonMode);
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

        return Objects.equals(operand1, that.operand1) && Objects.equals(operand2, that.operand2)
                   && Objects.equals(type, that.type) && comparisonMode == that.comparisonMode;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mode=" + comparisonMode + ", operand1=" + operand1 + ", operand2=" + operand2 + '}';
    }
}
