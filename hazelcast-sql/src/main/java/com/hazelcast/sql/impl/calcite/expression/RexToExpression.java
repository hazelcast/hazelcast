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

package com.hazelcast.sql.impl.calcite.expression;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.calcite.operators.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.SqlToQueryType;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.datetime.CurrentDateFunction;
import com.hazelcast.sql.impl.expression.datetime.CurrentTimestampFunction;
import com.hazelcast.sql.impl.expression.datetime.DatePartFunction;
import com.hazelcast.sql.impl.expression.datetime.DatePartUnit;
import com.hazelcast.sql.impl.expression.datetime.DatePartUnitConstantExpression;
import com.hazelcast.sql.impl.expression.datetime.LocalTimeFunction;
import com.hazelcast.sql.impl.expression.datetime.LocalTimestampFunction;
import com.hazelcast.sql.impl.expression.math.AbsFunction;
import com.hazelcast.sql.impl.expression.math.Atan2Function;
import com.hazelcast.sql.impl.expression.math.DivideFunction;
import com.hazelcast.sql.impl.expression.math.DoubleFunction;
import com.hazelcast.sql.impl.expression.math.DoubleFunctionType;
import com.hazelcast.sql.impl.expression.math.FloorCeilFunction;
import com.hazelcast.sql.impl.expression.math.MinusFunction;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.expression.math.PowerFunction;
import com.hazelcast.sql.impl.expression.math.RandomFunction;
import com.hazelcast.sql.impl.expression.math.RemainderFunction;
import com.hazelcast.sql.impl.expression.math.RoundTruncateFunction;
import com.hazelcast.sql.impl.expression.math.SignFunction;
import com.hazelcast.sql.impl.expression.math.UnaryMinusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.CaseExpression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.NotPredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import com.hazelcast.sql.impl.expression.string.AsciiFunction;
import com.hazelcast.sql.impl.expression.string.CharLengthFunction;
import com.hazelcast.sql.impl.expression.string.ConcatFunction;
import com.hazelcast.sql.impl.expression.string.InitcapFunction;
import com.hazelcast.sql.impl.expression.string.LikeFunction;
import com.hazelcast.sql.impl.expression.string.LowerFunction;
import com.hazelcast.sql.impl.expression.string.PositionFunction;
import com.hazelcast.sql.impl.expression.string.ReplaceFunction;
import com.hazelcast.sql.impl.expression.string.SubstringFunction;
import com.hazelcast.sql.impl.expression.string.UpperFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Calendar;

/**
 * Provides utility methods for REX to expression conversion.
 */
public final class RexToExpression {

    private static final int MILLISECONDS_PER_SECOND = 1_000;
    private static final int NANOSECONDS_PER_MILLISECOND = 1_000_000;

    private RexToExpression() {
        // No-op.
    }

    /**
     * Converts a {@link RexCall} to {@link Expression}.
     *
     * @param call the call to convert.
     * @return the resulting expression.
     * @throws HazelcastSqlException if the given {@link RexCall} can't be
     *                               converted.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength", "checkstyle:ReturnCount",
            "checkstyle:NPathComplexity"})
    public static Expression<?> convertCall(RexCall call, Expression<?>[] operands) {
        SqlOperator operator = call.getOperator();

        QueryDataType returnType = SqlToQueryType.map(call.getType().getSqlTypeName());

        switch (operator.getKind()) {
            case PLUS:
                return PlusFunction.create(operands[0], operands[1]);

            case MINUS:
                return MinusFunction.create(operands[0], operands[1]);

            case TIMES:
                return MultiplyFunction.create(operands[0], operands[1]);

            case DIVIDE:
                return DivideFunction.create(operands[0], operands[1]);

            case MOD:
                return RemainderFunction.create(operands[0], operands[1]);

            case MINUS_PREFIX:
                return UnaryMinusFunction.create(operands[0]);

            case FLOOR:
                return FloorCeilFunction.create(operands[0], false);

            case CEIL:
                return FloorCeilFunction.create(operands[0], true);

            case POSITION:
                Expression<?> position = operands.length == 2 ? null : operands[2];
                return PositionFunction.create(operands[0], operands[1], position);

            case OTHER:
                if (operator == SqlStdOperatorTable.CONCAT) {
                    return ConcatFunction.create(operands[0], operands[1]);
                }

                break;

            case EXTRACT:
                DatePartUnit unit = ((DatePartUnitConstantExpression) operands[0]).getUnit();
                return DatePartFunction.create(operands[1], unit);

            case TIMESTAMP_ADD:
                // TODO
                return null;

            case IS_NULL:
                return IsNullPredicate.create(operands[0]);

            case IS_NOT_NULL:
                return IsNotNullPredicate.create(operands[0]);

            case IS_FALSE:
                return IsFalsePredicate.create(operands[0]);

            case IS_NOT_FALSE:
                return IsNotFalsePredicate.create(operands[0]);

            case IS_TRUE:
                return IsTruePredicate.create(operands[0]);

            case IS_NOT_TRUE:
                return IsNotTruePredicate.create(operands[0]);

            case AND:
                return AndPredicate.create(operands);

            case OR:
                return OrPredicate.create(operands);

            case NOT:
                return NotPredicate.create(operands[0]);

            case EQUALS:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.EQUALS);

            case NOT_EQUALS:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.NOT_EQUALS);

            case GREATER_THAN:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.GREATER_THAN);

            case GREATER_THAN_OR_EQUAL:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.GREATER_THAN_OR_EQUAL);

            case LESS_THAN:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.LESS_THAN);

            case LESS_THAN_OR_EQUAL:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.LESS_THAN_OR_EQUAL);

            case CASE:
                return CaseExpression.create(operands);

            case LIKE:
                Expression<?> escape = operands.length == 2 ? null : operands[2];
                return LikeFunction.create(operands[0], operands[1], escape);

            case OTHER_FUNCTION:
                SqlFunction function = (SqlFunction) operator;

                // Math.

                if (function == SqlStdOperatorTable.COS) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.COS);
                } else if (function == SqlStdOperatorTable.SIN) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.SIN);
                } else if (function == SqlStdOperatorTable.TAN) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.TAN);
                } else if (function == SqlStdOperatorTable.COT) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.COT);
                } else if (function == SqlStdOperatorTable.ACOS) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.ACOS);
                } else if (function == SqlStdOperatorTable.ASIN) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.ASIN);
                } else if (function == SqlStdOperatorTable.ATAN) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.ATAN);
                } else if (function == SqlStdOperatorTable.SQRT) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.SQRT);
                } else if (function == SqlStdOperatorTable.EXP) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.EXP);
                } else if (function == SqlStdOperatorTable.LN) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.LN);
                } else if (function == SqlStdOperatorTable.LOG10) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.LOG10);
                } else if (function == SqlStdOperatorTable.RAND) {
                    return RandomFunction.create(operands.length == 0 ? null : operands[0]);
                } else if (function == SqlStdOperatorTable.ABS) {
                    return AbsFunction.create(operands[0]);
                } else if (function == SqlStdOperatorTable.PI) {
                    return ConstantExpression.create(returnType, Math.PI);
                } else if (function == SqlStdOperatorTable.SIGN) {
                    return SignFunction.create(operands[0]);
                } else if (function == SqlStdOperatorTable.ATAN2) {
                    return Atan2Function.create(operands[0], operands[1]);
                } else if (function == SqlStdOperatorTable.POWER) {
                    return PowerFunction.create(operands[0], operands[1]);
                } else if (function == SqlStdOperatorTable.DEGREES) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.DEGREES);
                } else if (function == SqlStdOperatorTable.RADIANS) {
                    return DoubleFunction.create(operands[0], DoubleFunctionType.RADIANS);
                } else if (function == SqlStdOperatorTable.ROUND) {
                    return RoundTruncateFunction.create(operands[0], operands.length == 1 ? null : operands[0], false);
                } else if (function == SqlStdOperatorTable.TRUNCATE) {
                    return RoundTruncateFunction.create(operands[0], operands.length == 1 ? null : operands[0], true);
                }

                // Strings.

                if (function == SqlStdOperatorTable.CHAR_LENGTH || function == SqlStdOperatorTable.CHARACTER_LENGTH
                        || function == HazelcastSqlOperatorTable.LENGTH) {
                    return CharLengthFunction.create(operands[0]);
                } else if (function == SqlStdOperatorTable.UPPER) {
                    return UpperFunction.create(operands[0]);
                } else if (function == SqlStdOperatorTable.LOWER) {
                    return LowerFunction.create(operands[0]);
                } else if (function == SqlStdOperatorTable.INITCAP) {
                    return InitcapFunction.create(operands[0]);
                } else if (function == SqlStdOperatorTable.ASCII) {
                    return AsciiFunction.create(operands[0]);
                } else if (function == SqlStdOperatorTable.REPLACE) {
                    return ReplaceFunction.create(operands[0], operands[1], operands[2]);
                } else if (function == SqlStdOperatorTable.SUBSTRING) {
                    return SubstringFunction.create(operands[0], operands[1], operands[2]);
                }

                // Dates.

                if (function == SqlStdOperatorTable.CURRENT_DATE) {
                    return new CurrentDateFunction();
                } else if (function == SqlStdOperatorTable.CURRENT_TIMESTAMP) {
                    return CurrentTimestampFunction.create(operands.length == 0 ? null : operands[0]);
                } else if (function == SqlStdOperatorTable.LOCALTIMESTAMP) {
                    return LocalTimestampFunction.create(operands.length == 0 ? null : operands[0]);
                } else if (function == SqlStdOperatorTable.LOCALTIME) {
                    return LocalTimeFunction.create(operands.length == 0 ? null : operands[0]);
                }

                break;

            default:
                break;
        }

        throw HazelcastSqlException.error("Unsupported operator: " + operator);
    }

    /**
     * Convert literal to simple object.
     *
     * @param literal Literal.
     * @return Object.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public static Expression<?> convertLiteral(RexLiteral literal) {
        SqlTypeName type = literal.getType().getSqlTypeName();

        switch (type) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case FLOAT:
            case DOUBLE:
                return convertNumericLiteral(literal, type);

            case CHAR:
            case VARCHAR:
                return convertStringLiteral(literal, type);

            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTemporalLiteral(literal, type);

            case INTERVAL_YEAR:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
                return convertIntervalYearMonthLiteral(literal, type);

            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return convertDaySecondLiteral(literal, type);

            case SYMBOL:
                return convertSymbolLiteral(literal);

            case NULL:
                return ConstantExpression.create(QueryDataType.NULL, null);

            default:
                throw HazelcastSqlException.error("Unsupported literal: " + literal);
        }
    }

    private static Expression<?> convertNumericLiteral(RexLiteral literal, SqlTypeName type) {
        Object value;
        switch (type) {
            case BOOLEAN:
                value = literal.getValueAs(Boolean.class);
                break;

            case TINYINT:
                value = literal.getValueAs(Byte.class);
                break;

            case SMALLINT:
                value = literal.getValueAs(Short.class);
                break;

            case INTEGER:
                value = literal.getValueAs(Integer.class);
                break;

            case BIGINT:
                value = literal.getValueAs(Long.class);
                break;

            case DECIMAL:
                value = literal.getValueAs(BigDecimal.class);
                break;

            case REAL:
                value = literal.getValueAs(Float.class);
                break;

            case FLOAT:
            case DOUBLE:
                value = literal.getValueAs(Double.class);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(SqlToQueryType.map(type), value);
    }

    private static Expression<?> convertStringLiteral(RexLiteral literal, SqlTypeName type) {
        Object value;
        switch (type) {
            case CHAR:
            case VARCHAR:
                value = literal.getValueAs(String.class);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(SqlToQueryType.map(type), value);
    }

    private static Expression<?> convertTemporalLiteral(RexLiteral literal, SqlTypeName type) {
        Calendar calendar = literal.getValueAs(Calendar.class);
        CalendarConverter converter = CalendarConverter.INSTANCE;

        Object value;
        switch (type) {
            case DATE:
                value = converter.asDate(calendar);
                break;

            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                value = converter.asTime(calendar);
                break;

            case TIMESTAMP:
                value = converter.asTimestamp(calendar);
                break;

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                value = converter.asTimestampWithTimezone(calendar);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(SqlToQueryType.map(type), value);
    }

    private static Expression<?> convertIntervalYearMonthLiteral(RexLiteral literal, SqlTypeName type) {
        int months = literal.getValueAs(Integer.class);
        return ConstantExpression.create(SqlToQueryType.map(type), new SqlYearMonthInterval(months));
    }

    private static Expression<?> convertDaySecondLiteral(RexLiteral literal, SqlTypeName type) {
        long value = literal.getValueAs(Long.class);

        long seconds = value / MILLISECONDS_PER_SECOND;
        int nanoseconds = (int) (value % MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND;

        return ConstantExpression.create(SqlToQueryType.map(type), new SqlDaySecondInterval(seconds, nanoseconds));
    }

    private static Expression<?> convertSymbolLiteral(RexLiteral literal) {
        Object value = literal.getValue();

        if (value instanceof TimeUnitRange) {
            TimeUnitRange range = (TimeUnitRange) value;

            DatePartUnit unit;
            switch (range) {
                case YEAR:
                    unit = DatePartUnit.YEAR;
                    break;

                case QUARTER:
                    unit = DatePartUnit.QUARTER;
                    break;

                case MONTH:
                    unit = DatePartUnit.MONTH;
                    break;

                case WEEK:
                    unit = DatePartUnit.WEEK;
                    break;

                case DOY:
                    unit = DatePartUnit.DAYOFYEAR;

                    break;

                case DOW:
                    unit = DatePartUnit.DAYOFWEEK;
                    break;

                case DAY:
                    unit = DatePartUnit.DAYOFMONTH;
                    break;

                case HOUR:
                    unit = DatePartUnit.HOUR;
                    break;

                case MINUTE:
                    unit = DatePartUnit.MINUTE;
                    break;

                case SECOND:
                    unit = DatePartUnit.SECOND;
                    break;

                default:
                    throw HazelcastSqlException.error("Unsupported literal symbol: " + literal);
            }

            return new DatePartUnitConstantExpression(unit);
        }

        throw HazelcastSqlException.error("Unsupported literal symbol: " + literal);
    }

}
