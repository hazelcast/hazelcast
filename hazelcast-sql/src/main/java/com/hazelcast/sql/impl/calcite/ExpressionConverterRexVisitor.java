/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlDaySecondInterval;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlYearMonthInterval;
import com.hazelcast.sql.impl.calcite.operators.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExtractorExpression;
import com.hazelcast.sql.impl.expression.ItemExpression;
import com.hazelcast.sql.impl.expression.math.AbsFunction;
import com.hazelcast.sql.impl.expression.math.DivideRemainderFunction;
import com.hazelcast.sql.impl.expression.math.FloorCeilFunction;
import com.hazelcast.sql.impl.expression.math.MathBiFunction;
import com.hazelcast.sql.impl.expression.math.MathUniFunction;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.expression.math.PlusMinusFunction;
import com.hazelcast.sql.impl.expression.math.RandomFunction;
import com.hazelcast.sql.impl.expression.math.RoundTruncateFunction;
import com.hazelcast.sql.impl.expression.math.SignFunction;
import com.hazelcast.sql.impl.expression.math.UnaryMinusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndOrPredicate;
import com.hazelcast.sql.impl.expression.predicate.CaseExpression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsPredicate;
import com.hazelcast.sql.impl.expression.predicate.NotPredicate;
import com.hazelcast.sql.impl.expression.string.ConcatFunction;
import com.hazelcast.sql.impl.expression.string.PositionFunction;
import com.hazelcast.sql.impl.expression.string.ReplaceFunction;
import com.hazelcast.sql.impl.expression.string.StringFunction;
import com.hazelcast.sql.impl.expression.string.SubstringFunction;
import com.hazelcast.sql.impl.expression.time.CurrentDateFunction;
import com.hazelcast.sql.impl.expression.time.DatePartFunction;
import com.hazelcast.sql.impl.expression.time.DatePartUnit;
import com.hazelcast.sql.impl.expression.time.GetTimestampFunction;
import com.hazelcast.sql.impl.type.accessor.CalendarConverter;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

/**
 * Visitor which converts REX node to a Hazelcast expression.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public final class ExpressionConverterRexVisitor implements RexVisitor<Expression> {
    /** Singleton. */
    public static final ExpressionConverterRexVisitor INSTANCE = new ExpressionConverterRexVisitor();

    private ExpressionConverterRexVisitor() {
        // No-op.
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
        int idx = inputRef.getIndex();

        return new ColumnExpression(idx);
    }

    @Override
    public Expression visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
        Object convertedLiteral = convertLiteral(literal);

        return new ConstantExpression<>(convertedLiteral);
    }

    /**
     * Convert literal to simple object.
     *
     * @param literal Literal.
     * @return Object.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    private Object convertLiteral(RexLiteral literal) {
        switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN:
                return literal.getValueAs(Boolean.class);

            case TINYINT:
                return literal.getValueAs(Byte.class);

            case SMALLINT:
                return literal.getValueAs(Short.class);

            case INTEGER:
                return literal.getValueAs(Integer.class);

            case BIGINT:
                return literal.getValueAs(Long.class);

            case DECIMAL:
                return literal.getValueAs(BigDecimal.class);

            case REAL:
                return literal.getValueAs(Float.class);

            case FLOAT:
            case DOUBLE:
                return literal.getValueAs(Double.class);

            case CHAR:
            case VARCHAR:
                return literal.getValueAs(String.class);

            case SYMBOL:
                return convertSymbol(literal);

            case DATE:
                Calendar val = (Calendar) literal.getValue();

                return CalendarConverter.INSTANCE.asDate(val);

            // TODO: Time, Timestamp, etc.

            case INTERVAL_YEAR:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
                return convertYearMonthInterval(literal);

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
                return convertDaySecondInterval(literal);

            default:
                throw new HazelcastSqlException(-1, "Unsupported literal: " + literal);
        }
    }

    private SqlYearMonthInterval convertYearMonthInterval(RexLiteral literal) {
        int months = literal.getValueAs(Integer.class);

        return new SqlYearMonthInterval(months);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private SqlDaySecondInterval convertDaySecondInterval(RexLiteral literal) {
        long val = literal.getValueAs(Long.class);

        long sec = val / 1_000;
        int nano = (int) (val % 1_000) * 1_000_000;

        return new SqlDaySecondInterval(sec, nano);
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private Object convertSymbol(RexLiteral literal) {
        Object literalValue = literal.getValue();

        if (literalValue instanceof TimeUnitRange) {
            TimeUnitRange unit = (TimeUnitRange) literalValue;

            switch (unit) {
                case YEAR:
                    return DatePartUnit.YEAR;

                case QUARTER:
                    return DatePartUnit.QUARTER;

                case MONTH:
                    return DatePartUnit.MONTH;

                case WEEK:
                    return DatePartUnit.WEEK;

                case DOY:
                    return DatePartUnit.DAYOFYEAR;

                case DOW:
                    return DatePartUnit.DAYOFWEEK;

                case DAY:
                    return DatePartUnit.DAYOFMONTH;

                case HOUR:
                    return DatePartUnit.HOUR;

                case MINUTE:
                    return DatePartUnit.MINUTE;

                case SECOND:
                    return DatePartUnit.SECOND;

                default:
                    break;
            }
        }

        throw new HazelcastSqlException(-1, "Unsupported literal symbol: " + literal);
    }

    @SuppressWarnings({"unchecked", "checkstyle:CyclomaticComplexity", "checkstyle:MethodLength",
        "checkstyle:NPathComplexity", "checkstyle:ReturnCount"})
    @Override
    public Expression visitCall(RexCall call) {
        // Convert operator.
        SqlOperator operator = call.getOperator();

        int hzOperator = convertOperator(operator);

        // Convert operands.
        List<RexNode> operands = call.getOperands();

        List<Expression> hzOperands;

        if (operands == null || operands.isEmpty()) {
            hzOperands = Collections.emptyList();
        } else {
            hzOperands = new ArrayList<>(operands.size());

            for (RexNode operand : operands) {
                Expression convertedOperand = operand.accept(this);

                hzOperands.add(convertedOperand);
            }
        }

        switch (hzOperator) {
            case CallOperator.PLUS:
                return new PlusMinusFunction(hzOperands.get(0), hzOperands.get(1), false);

            case CallOperator.MINUS:
                return new PlusMinusFunction(hzOperands.get(0), hzOperands.get(1), true);

            case CallOperator.MULTIPLY:
                return new MultiplyFunction(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.DIVIDE:
                return new DivideRemainderFunction(hzOperands.get(0), hzOperands.get(1), false);

            case CallOperator.REMAINDER:
                return new DivideRemainderFunction(hzOperands.get(0), hzOperands.get(1), true);

            case CallOperator.UNARY_MINUS:
                return new UnaryMinusFunction(hzOperands.get(0));

            case CallOperator.CHAR_LENGTH:
            case CallOperator.ASCII:
            case CallOperator.UPPER:
            case CallOperator.LOWER:
            case CallOperator.INITCAP:
                return new StringFunction(hzOperands.get(0), hzOperator);

            case CallOperator.CONCAT:
                return new ConcatFunction(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.POSITION:
                if (hzOperands.size() == 2) {
                    return new PositionFunction(hzOperands.get(0), hzOperands.get(1), null);
                } else {
                    return new PositionFunction(hzOperands.get(0), hzOperands.get(1), hzOperands.get(2));
                }

            case CallOperator.REPLACE:
                return new ReplaceFunction(hzOperands.get(0), hzOperands.get(1), hzOperands.get(2));

            case CallOperator.SUBSTRING:
                return new SubstringFunction(hzOperands.get(0), hzOperands.get(1), hzOperands.get(2));

            case CallOperator.COS:
            case CallOperator.SIN:
            case CallOperator.TAN:
            case CallOperator.COT:
            case CallOperator.ACOS:
            case CallOperator.ASIN:
            case CallOperator.ATAN:
            case CallOperator.SQRT:
            case CallOperator.EXP:
            case CallOperator.LN:
            case CallOperator.LOG10:
            case CallOperator.DEGREES:
            case CallOperator.RADIANS:
                return new MathUniFunction(hzOperands.get(0), hzOperator);

            case CallOperator.FLOOR:
                return new FloorCeilFunction(hzOperands.get(0), false);

            case CallOperator.CEIL:
                return new FloorCeilFunction(hzOperands.get(0), true);

            case CallOperator.ROUND:
                if (hzOperands.size() == 1) {
                    return new RoundTruncateFunction(hzOperands.get(0), null, false);
                } else {
                    return new RoundTruncateFunction(hzOperands.get(0), hzOperands.get(1), false);
                }

            case CallOperator.TRUNCATE:
                if (hzOperands.size() == 1) {
                    return new RoundTruncateFunction(hzOperands.get(0), null, true);
                } else {
                    return new RoundTruncateFunction(hzOperands.get(0), hzOperands.get(1), true);
                }

            case CallOperator.RAND:
                if (hzOperands.isEmpty()) {
                    return new RandomFunction();
                } else {
                    assert hzOperands.size() == 1;

                    return new RandomFunction(hzOperands.get(0));
                }

            case CallOperator.ABS:
                return new AbsFunction(hzOperands.get(0));

            case CallOperator.PI:
                return new ConstantExpression<>(Math.PI);

            case CallOperator.SIGN:
                return new SignFunction(hzOperands.get(0));

            case CallOperator.ATAN2:
            case CallOperator.POWER:
                return new MathBiFunction(hzOperands.get(0), hzOperands.get(1), hzOperator);

            case CallOperator.EXTRACT:
                DatePartUnit unit = ((ConstantExpression<DatePartUnit>) hzOperands.get(0)).getValue();

                return new DatePartFunction(hzOperands.get(1), unit);

            case CallOperator.TIMESTAMP_ADD:
                return null;

            case CallOperator.CURRENT_DATE:
                return new CurrentDateFunction();

            case CallOperator.CURRENT_TIMESTAMP:
            case CallOperator.LOCAL_TIMESTAMP:
            case CallOperator.LOCAL_TIME:
                return new GetTimestampFunction(hzOperands.isEmpty() ? null : hzOperands.get(0), hzOperator);

            case CallOperator.IS_NULL:
            case CallOperator.IS_NOT_NULL:
            case CallOperator.IS_FALSE:
            case CallOperator.IS_NOT_FALSE:
            case CallOperator.IS_TRUE:
            case CallOperator.IS_NOT_TRUE:
                return new IsPredicate(hzOperands.get(0), hzOperator);

            case CallOperator.AND:
                return new AndOrPredicate(hzOperands.get(0), hzOperands.get(1), false);

            case CallOperator.OR:
                return new AndOrPredicate(hzOperands.get(0), hzOperands.get(1), true);

            case CallOperator.NOT:
                return new NotPredicate(hzOperands.get(0));

            case CallOperator.EQUALS:
            case CallOperator.NOT_EQUALS:
            case CallOperator.GREATER_THAN:
            case CallOperator.GREATER_THAN_EQUAL:
            case CallOperator.LESS_THAN:
            case CallOperator.LESS_THAN_EQUAL:
                return new ComparisonPredicate(hzOperands.get(0), hzOperands.get(1), hzOperator);

            case CallOperator.CASE:
                return new CaseExpression(hzOperands);

            case CallOperator.ITEM:
                return new ItemExpression(hzOperands.get(0), hzOperands.get(1));

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Unsupported operator: " + operator);
        }
    }

    @Override
    public Expression visitOver(RexOver over) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
        RexNode referenceExpr = fieldAccess.getReferenceExpr();

        Expression convertedReferenceExpr = referenceExpr.accept(this);
        String path = fieldAccess.getField().getName();

        return new ExtractorExpression<>(convertedReferenceExpr, path);
    }

    @Override
    public Expression visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitTableInputRef(RexTableInputRef fieldRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedOperationException();
    }

    /**
     * Convert Calcite operator to Hazelcast operator.
     *
     * @param operator Calcite operator.
     * @return Hazelcast operator.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength", "checkstyle:NPathComplexity",
        "checkstyle:ReturnCount", "checkstyle:AvoidNestedBlocks"})
    private static int convertOperator(SqlOperator operator) {
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

            case SELECT:
                break;

            case JOIN:
                break;

            case IDENTIFIER:
                break;

            case LITERAL:
                break;

            case OTHER_FUNCTION: {
                if ("ITEM".equals(operator.getName())) {
                    return CallOperator.ITEM;
                }

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

        throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
    }
}
