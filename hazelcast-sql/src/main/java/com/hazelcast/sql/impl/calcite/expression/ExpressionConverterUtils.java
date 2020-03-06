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
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import com.hazelcast.sql.impl.calcite.operators.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.expression.CallOperator;
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
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;

/**
 * Utility method for expression converter.
 */
public final class ExpressionConverterUtils {
    private ExpressionConverterUtils() {
        // No-op.
    }

    /**
     * Convert Calcite operator to Hazelcast operator.
     *
     * @param operator Calcite operator.
     * @return Hazelcast operator.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength", "checkstyle:NPathComplexity",
        "checkstyle:ReturnCount", "checkstyle:AvoidNestedBlocks"})
    public static int convertOperator(SqlOperator operator) {
        switch (operator.getKind()) {
            case PLUS:
                return CallOperator.PLUS;

            case MINUS:
                return CallOperator.MINUS;

            case TIMES:
                return CallOperator.MULTIPLY;

            case DIVIDE:
                return CallOperator.DIVIDE;

            case MOD:
                return CallOperator.REMAINDER;

            case MINUS_PREFIX:
                return CallOperator.UNARY_MINUS;

            case FLOOR:
                return CallOperator.FLOOR;

            case CEIL:
                return CallOperator.CEIL;

            case POSITION:
                return CallOperator.POSITION;

            case OTHER:
                if (operator == SqlStdOperatorTable.CONCAT) {
                    return CallOperator.CONCAT;
                }

                break;

            case EXTRACT:
                return CallOperator.EXTRACT;

            case TIMESTAMP_ADD:
                return CallOperator.TIMESTAMP_ADD;

            case IS_NULL:
                return CallOperator.IS_NULL;

            case IS_NOT_NULL:
                return CallOperator.IS_NOT_NULL;

            case IS_FALSE:
                return CallOperator.IS_FALSE;

            case IS_NOT_FALSE:
                return CallOperator.IS_NOT_FALSE;

            case IS_TRUE:
                return CallOperator.IS_TRUE;

            case IS_NOT_TRUE:
                return CallOperator.IS_NOT_TRUE;

            case AND:
                return CallOperator.AND;

            case OR:
                return CallOperator.OR;

            case NOT:
                return CallOperator.NOT;

            case EQUALS:
                return CallOperator.EQUALS;

            case NOT_EQUALS:
                return CallOperator.NOT_EQUALS;

            case GREATER_THAN:
                return CallOperator.GREATER_THAN;

            case GREATER_THAN_OR_EQUAL:
                return CallOperator.GREATER_THAN_EQUAL;

            case LESS_THAN:
                return CallOperator.LESS_THAN;

            case LESS_THAN_OR_EQUAL:
                return CallOperator.LESS_THAN_EQUAL;

            case CASE:
                return CallOperator.CASE;

            case LIKE:
                return CallOperator.LIKE;

            case OTHER_FUNCTION: {
                SqlFunction function = (SqlFunction) operator;

                if (function == SqlStdOperatorTable.COS) {
                    return CallOperator.COS;
                } else if (function == SqlStdOperatorTable.SIN) {
                    return CallOperator.SIN;
                } else if (function == SqlStdOperatorTable.TAN) {
                    return CallOperator.COT;
                } else if (function == SqlStdOperatorTable.COT) {
                    return CallOperator.COT;
                } else if (function == SqlStdOperatorTable.ACOS) {
                    return CallOperator.ACOS;
                } else if (function == SqlStdOperatorTable.ASIN) {
                    return CallOperator.ASIN;
                } else if (function == SqlStdOperatorTable.ATAN) {
                    return CallOperator.ATAN;
                } else if (function == SqlStdOperatorTable.SQRT) {
                    return CallOperator.SQRT;
                } else if (function == SqlStdOperatorTable.EXP) {
                    return CallOperator.EXP;
                } else if (function == SqlStdOperatorTable.LN) {
                    return CallOperator.LN;
                } else if (function == SqlStdOperatorTable.LOG10) {
                    return CallOperator.LOG10;
                } else if (function == SqlStdOperatorTable.RAND) {
                    return CallOperator.RAND;
                } else if (function == SqlStdOperatorTable.ABS) {
                    return CallOperator.ABS;
                } else if (function == SqlStdOperatorTable.PI) {
                    return CallOperator.PI;
                } else if (function == SqlStdOperatorTable.SIGN) {
                    return CallOperator.SIGN;
                } else if (function == SqlStdOperatorTable.ATAN2) {
                    return CallOperator.ATAN2;
                } else if (function == SqlStdOperatorTable.POWER) {
                    return CallOperator.POWER;
                } else if (function == SqlStdOperatorTable.DEGREES) {
                    return CallOperator.DEGREES;
                } else if (function == SqlStdOperatorTable.RADIANS) {
                    return CallOperator.RADIANS;
                } else if (function == SqlStdOperatorTable.ROUND) {
                    return CallOperator.ROUND;
                } else if (function == SqlStdOperatorTable.TRUNCATE) {
                    return CallOperator.TRUNCATE;
                }

                if (function == SqlStdOperatorTable.CHAR_LENGTH
                        || function == SqlStdOperatorTable.CHARACTER_LENGTH
                        || function == HazelcastSqlOperatorTable.LENGTH
                ) {
                    return CallOperator.CHAR_LENGTH;
                } else if (function == SqlStdOperatorTable.UPPER) {
                    return CallOperator.UPPER;
                } else if (function == SqlStdOperatorTable.LOWER) {
                    return CallOperator.LOWER;
                } else if (function == SqlStdOperatorTable.INITCAP) {
                    return CallOperator.INITCAP;
                } else if (function == SqlStdOperatorTable.ASCII) {
                    return CallOperator.ASCII;
                } else if (function == SqlStdOperatorTable.REPLACE) {
                    return CallOperator.REPLACE;
                } else if (function == SqlStdOperatorTable.SUBSTRING) {
                    return CallOperator.SUBSTRING;
                } else if (function == SqlStdOperatorTable.CURRENT_DATE) {
                    return CallOperator.CURRENT_DATE;
                } else if (function == SqlStdOperatorTable.CURRENT_TIMESTAMP) {
                    return CallOperator.CURRENT_TIMESTAMP;
                } else if (function == SqlStdOperatorTable.LOCALTIMESTAMP) {
                    return CallOperator.LOCAL_TIMESTAMP;
                } else if (function == SqlStdOperatorTable.LOCALTIME) {
                    return CallOperator.LOCAL_TIME;
                }

                break;
            }

            default:
                break;
        }

        throw HazelcastSqlException.error("Unsupported operator: " + operator);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength", "checkstyle:ReturnCount"})
    public static Expression<?> convertCall(int operator, List<Expression<?>> operands) {
        switch (operator) {
            case CallOperator.PLUS:
                return PlusFunction.create(operands.get(0), operands.get(1));

            case CallOperator.MINUS:
                return MinusFunction.create(operands.get(0), operands.get(1));

            case CallOperator.MULTIPLY:
                return MultiplyFunction.create(operands.get(0), operands.get(1));

            case CallOperator.DIVIDE:
                return DivideFunction.create(operands.get(0), operands.get(1));

            case CallOperator.REMAINDER:
                return RemainderFunction.create(operands.get(0), operands.get(1));

            case CallOperator.UNARY_MINUS:
                return UnaryMinusFunction.create(operands.get(0));

            case CallOperator.CHAR_LENGTH:
                return CharLengthFunction.create(operands.get(0));

            case CallOperator.ASCII:
                return AsciiFunction.create(operands.get(0));

            case CallOperator.UPPER:
                return UpperFunction.create(operands.get(0));

            case CallOperator.LOWER:
                return LowerFunction.create(operands.get(0));

            case CallOperator.INITCAP:
                return InitcapFunction.create(operands.get(0));

            case CallOperator.CONCAT:
                return ConcatFunction.create(operands.get(0), operands.get(1));

            case CallOperator.POSITION:
                Expression<?> position = operands.size() == 2 ? null : operands.get(2);

                return PositionFunction.create(operands.get(0), operands.get(1), position);

            case CallOperator.REPLACE:
                return ReplaceFunction.create(operands.get(0), operands.get(1), operands.get(2));

            case CallOperator.SUBSTRING:
                return SubstringFunction.create(operands.get(0), operands.get(1), operands.get(2));

            case CallOperator.LIKE:
                Expression<?> escape = operands.size() == 2 ? null : operands.get(2);

                return LikeFunction.create(operands.get(0), operands.get(1), escape);

            case CallOperator.COS:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.COS);

            case CallOperator.SIN:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.SIN);

            case CallOperator.TAN:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.TAN);

            case CallOperator.COT:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.COT);

            case CallOperator.ACOS:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.ACOS);

            case CallOperator.ASIN:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.ASIN);

            case CallOperator.ATAN:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.ATAN);

            case CallOperator.SQRT:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.SQRT);

            case CallOperator.EXP:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.EXP);

            case CallOperator.LN:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.LN);

            case CallOperator.LOG10:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.LOG10);

            case CallOperator.DEGREES:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.DEGREES);

            case CallOperator.RADIANS:
                return DoubleFunction.create(operands.get(0), DoubleFunctionType.RADIANS);

            case CallOperator.FLOOR:
                return FloorCeilFunction.create(operands.get(0), false);

            case CallOperator.CEIL:
                return FloorCeilFunction.create(operands.get(0), true);

            case CallOperator.ROUND:
                if (operands.size() == 1) {
                    return RoundTruncateFunction.create(operands.get(0), null, false);
                } else {
                    return RoundTruncateFunction.create(operands.get(0), operands.get(1), false);
                }

            case CallOperator.TRUNCATE:
                if (operands.size() == 1) {
                    return RoundTruncateFunction.create(operands.get(0), null, true);
                } else {
                    return RoundTruncateFunction.create(operands.get(0), operands.get(1), true);
                }

            case CallOperator.RAND:
                if (operands.isEmpty()) {
                    return RandomFunction.create(null);
                } else {
                    assert operands.size() == 1;

                    return RandomFunction.create(operands.get(0));
                }

            case CallOperator.ABS:
                return AbsFunction.create(operands.get(0));

            case CallOperator.PI:
                return ConstantExpression.create(Math.PI);

            case CallOperator.SIGN:
                return SignFunction.create(operands.get(0));

            case CallOperator.ATAN2:
                return Atan2Function.create(operands.get(0), operands.get(1));

            case CallOperator.POWER:
                return PowerFunction.create(operands.get(0), operands.get(1));

            case CallOperator.EXTRACT:
                DatePartUnit unit = ((DatePartUnitConstantExpression) operands.get(0)).getUnit();

                return DatePartFunction.create(operands.get(1), unit);

            case CallOperator.TIMESTAMP_ADD:
                return null;

            case CallOperator.CURRENT_DATE:
                return new CurrentDateFunction();

            case CallOperator.CURRENT_TIMESTAMP:
                return CurrentTimestampFunction.create(operands.isEmpty() ? null : operands.get(0));

            case CallOperator.LOCAL_TIMESTAMP:
                return LocalTimestampFunction.create(operands.isEmpty() ? null : operands.get(0));

            case CallOperator.LOCAL_TIME:
                return LocalTimeFunction.create(operands.isEmpty() ? null : operands.get(0));

            case CallOperator.IS_NULL:
                return IsNullPredicate.create(operands.get(0));

            case CallOperator.IS_NOT_NULL:
                return IsNotNullPredicate.create(operands.get(0));

            case CallOperator.IS_FALSE:
                return IsFalsePredicate.create(operands.get(0));

            case CallOperator.IS_NOT_FALSE:
                return IsNotFalsePredicate.create(operands.get(0));

            case CallOperator.IS_TRUE:
                return IsTruePredicate.create(operands.get(0));

            case CallOperator.IS_NOT_TRUE:
                return IsNotTruePredicate.create(operands.get(0));

            case CallOperator.AND:
                return AndPredicate.create(operands.get(0), operands.get(1));

            case CallOperator.OR:
                return OrPredicate.create(operands.get(0), operands.get(1));

            case CallOperator.NOT:
                return NotPredicate.create(operands.get(0));

            case CallOperator.EQUALS:
                return ComparisonPredicate.create(operands.get(0), operands.get(1), ComparisonMode.EQUALS);

            case CallOperator.NOT_EQUALS:
                return ComparisonPredicate.create(operands.get(0), operands.get(1), ComparisonMode.NOT_EQUALS);

            case CallOperator.GREATER_THAN:
                return ComparisonPredicate.create(operands.get(0), operands.get(1), ComparisonMode.GREATER_THAN);

            case CallOperator.GREATER_THAN_EQUAL:
                return ComparisonPredicate.create(operands.get(0), operands.get(1), ComparisonMode.GREATER_THAN_EQUAL);

            case CallOperator.LESS_THAN:
                return ComparisonPredicate.create(operands.get(0), operands.get(1), ComparisonMode.LESS_THAN);

            case CallOperator.LESS_THAN_EQUAL:
                return ComparisonPredicate.create(operands.get(0), operands.get(1), ComparisonMode.LESS_THAN_EQUAL);

            case CallOperator.CASE:
                return CaseExpression.create(operands);

            default:
                throw HazelcastSqlException.error("Unsupported operator: " + operator);
        }
    }

    /**
     * Convert literal to simple object.
     *
     * @param literal Literal.
     * @return Object.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
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
                return convertLiteralNumeric(type, literal);

            case CHAR:
            case VARCHAR:
                return convertLiteralString(type, literal);

            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertLiteralTemporal(type, literal);

            case INTERVAL_YEAR:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
                return convertLiteralIntervalYearMonth(literal);

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
                return convertLiteralDaySecond(literal);

            case SYMBOL:
                return convertLiteralSymbol(literal);

            default:
                throw HazelcastSqlException.error("Unsupported literal: " + literal);
        }
    }

    private static Expression<?> convertLiteralNumeric(SqlTypeName type, RexLiteral literal) {
        Object val;

        switch (type) {
            case BOOLEAN:
                val = literal.getValueAs(Boolean.class);

                break;

            case TINYINT:
                val = literal.getValueAs(Byte.class);

                break;

            case SMALLINT:
                val = literal.getValueAs(Short.class);

                break;

            case INTEGER:
                val = literal.getValueAs(Integer.class);

                break;

            case BIGINT:
                val = literal.getValueAs(Long.class);

                break;

            case DECIMAL:
                val = literal.getValueAs(BigDecimal.class);

                break;

            case REAL:
                val = literal.getValueAs(Float.class);

                break;

            case FLOAT:
            case DOUBLE:
                val = literal.getValueAs(Double.class);

                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(val);
    }

    private static Expression<?> convertLiteralString(SqlTypeName type, RexLiteral literal) {
        Object val;

        switch (type) {
            case CHAR:
            case VARCHAR:
                val = literal.getValueAs(String.class);

                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(val);
    }

    private static Expression<?> convertLiteralTemporal(SqlTypeName type, RexLiteral literal) {
        Calendar calendar = literal.getValueAs(Calendar.class);
        CalendarConverter converter = CalendarConverter.INSTANCE;

        Object val;

        switch (type) {
            case DATE:
                val = converter.asDate(calendar);

                break;

            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                val = converter.asTime(calendar);

                break;

            case TIMESTAMP:
                val = converter.asTimestamp(calendar);

                break;

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                val = converter.asTimestampWithTimezone(calendar);

                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(val);
    }

    private static Expression<?> convertLiteralIntervalYearMonth(RexLiteral literal) {
        int months = literal.getValueAs(Integer.class);

        return ConstantExpression.create(new SqlYearMonthInterval(months));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private static Expression<?> convertLiteralDaySecond(RexLiteral literal) {
        long val = literal.getValueAs(Long.class);

        long sec = val / 1_000;
        int nano = (int) (val % 1_000) * 1_000_000;

        return ConstantExpression.create(new SqlDaySecondInterval(sec, nano));
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static Expression<?> convertLiteralSymbol(RexLiteral literal) {
        Object literalValue = literal.getValue();

        if (literalValue instanceof TimeUnitRange) {
            TimeUnitRange unit = (TimeUnitRange) literalValue;
            DatePartUnit unit0;

            switch (unit) {
                case YEAR:
                    unit0 = DatePartUnit.YEAR;

                    break;

                case QUARTER:
                    unit0 = DatePartUnit.QUARTER;

                    break;

                case MONTH:
                    unit0 = DatePartUnit.MONTH;

                    break;

                case WEEK:
                    unit0 = DatePartUnit.WEEK;

                    break;

                case DOY:
                    unit0 = DatePartUnit.DAYOFYEAR;

                    break;

                case DOW:
                    unit0 = DatePartUnit.DAYOFWEEK;

                    break;

                case DAY:
                    unit0 = DatePartUnit.DAYOFMONTH;

                    break;

                case HOUR:
                    unit0 = DatePartUnit.HOUR;

                    break;

                case MINUTE:
                    unit0 = DatePartUnit.MINUTE;

                    break;

                case SECOND:
                    unit0 = DatePartUnit.SECOND;

                    break;

                default:
                    throw HazelcastSqlException.error("Unsupported literal symbol: " + literal);
            }

            return new DatePartUnitConstantExpression(unit0);
        }

        throw HazelcastSqlException.error("Unsupported literal symbol: " + literal);
    }
}
