package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.func.AbsFunction;
import com.hazelcast.sql.impl.expression.call.func.Atan2Function;
import com.hazelcast.sql.impl.expression.call.func.ConcatFunction;
import com.hazelcast.sql.impl.expression.call.func.DivideFunction;
import com.hazelcast.sql.impl.expression.call.func.DoubleFunction;
import com.hazelcast.sql.impl.expression.call.func.FloorCeilFunction;
import com.hazelcast.sql.impl.expression.call.func.MinusFunction;
import com.hazelcast.sql.impl.expression.call.func.MultiplyFunction;
import com.hazelcast.sql.impl.expression.call.func.PlusFunction;
import com.hazelcast.sql.impl.expression.call.func.PositionFunction;
import com.hazelcast.sql.impl.expression.call.func.PowerFunction;
import com.hazelcast.sql.impl.expression.call.func.RandomFunction;
import com.hazelcast.sql.impl.expression.call.func.RemainderFunction;
import com.hazelcast.sql.impl.expression.call.func.ReplaceFunction;
import com.hazelcast.sql.impl.expression.call.func.RoundTruncateFunction;
import com.hazelcast.sql.impl.expression.call.func.SignFunction;
import com.hazelcast.sql.impl.expression.call.func.StringRetIntFunction;
import com.hazelcast.sql.impl.expression.call.func.StringRetStringFunction;
import com.hazelcast.sql.impl.expression.call.func.UnaryMinusFunction;
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
import java.util.Collections;
import java.util.List;

/**
 * Visitor which converts REX node to a Hazelcast expression.
 */
public class ExpressionConverterRexVisitor implements RexVisitor<Expression> {
    /** Singleton. */
    public static final ExpressionConverterRexVisitor INSTANCE = new ExpressionConverterRexVisitor();

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
        int idx = inputRef.getIndex();

        return new ColumnExpression(idx);
    }

    @Override
    public Expression visitLocalRef(RexLocalRef localRef) {
        // TODO
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
        }

        throw new HazelcastSqlException(-1, "Unsupported literal: " + literal);
    }

    @Override
    public Expression visitCall(RexCall call) {
        // Convert operator.
        SqlOperator operator = call.getOperator();

        int hzOperator = convertOperator(operator);

        // Convert operands.
        List<RexNode> operands = call.getOperands();

        List<Expression> hzOperands;

        if (operands == null || operands.isEmpty())
            hzOperands = Collections.emptyList();
        else {
            hzOperands = new ArrayList<>(operands.size());

            for (RexNode operand : operands) {
                Expression convertedOperand = operand.accept(this);

                hzOperands.add(convertedOperand);
            }
        }

        switch (hzOperator) {
            case CallOperator.PLUS:
                return new PlusFunction(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.MINUS:
                return new MinusFunction(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.MULTIPLY:
                return new MultiplyFunction(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.DIVIDE:
                return new DivideFunction(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.REMAINDER:
                return new RemainderFunction(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.UNARY_MINUS:
                return new UnaryMinusFunction(hzOperands.get(0));

            case CallOperator.CHAR_LENGTH:
            case CallOperator.ASCII:
                return new StringRetIntFunction(hzOperands.get(0), hzOperator);

            case CallOperator.UPPER:
            case CallOperator.LOWER:
            case CallOperator.INITCAP:
                return new StringRetStringFunction(hzOperands.get(0), hzOperator);

            case CallOperator.CONCAT:
                return new ConcatFunction(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.POSITION:
                if (hzOperands.size() == 2)
                    return new PositionFunction(hzOperands.get(0), hzOperands.get(1), null);
                else
                    return new PositionFunction(hzOperands.get(0), hzOperands.get(1), hzOperands.get(2));

            case CallOperator.REPLACE:
                return new ReplaceFunction(hzOperands.get(0), hzOperands.get(1), hzOperands.get(2));

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
                return new DoubleFunction(hzOperands.get(0), hzOperator);

            case CallOperator.FLOOR:
                return new FloorCeilFunction(hzOperands.get(0), false);

            case CallOperator.CEIL:
                return new FloorCeilFunction(hzOperands.get(0), true);

            case CallOperator.ROUND:
                if (hzOperands.size() == 1)
                    return new RoundTruncateFunction(hzOperands.get(0), null, false);
                else
                    return new RoundTruncateFunction(hzOperands.get(0), hzOperands.get(1), false);

            case CallOperator.TRUNCATE:
                if (hzOperands.size() == 1)
                    return new RoundTruncateFunction(hzOperands.get(0), null, true);
                else
                    return new RoundTruncateFunction(hzOperands.get(0), hzOperands.get(1), true);

            case CallOperator.RAND:
                if (hzOperands.isEmpty())
                    return new RandomFunction();
                else {
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
                return new Atan2Function(hzOperands.get(0), hzOperands.get(1));

            case CallOperator.POWER:
                return new PowerFunction(hzOperands.get(0), hzOperands.get(1));

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Unsupported operator: " + operator);
        }
    }

    @Override
    public Expression visitOver(RexOver over) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
        return null;
    }

    @Override
    public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitRangeRef(RexRangeRef rangeRef) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitSubQuery(RexSubQuery subQuery) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitTableInputRef(RexTableInputRef fieldRef) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        // TODO
        throw new UnsupportedOperationException();
    }

    /**
     * Convert Calcite operator to Hazelcast operator.
     *
     * @param operator Calcite operator.
     * @return Hazelcast operator.
     */
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
                if (operator == SqlStdOperatorTable.CONCAT)
                    return CallOperator.CONCAT;

            case OTHER_FUNCTION: {
                SqlFunction function = (SqlFunction)operator;

                if (function == SqlStdOperatorTable.COS)
                    return CallOperator.COS;
                else if (function == SqlStdOperatorTable.SIN)
                    return CallOperator.SIN;
                else if (function == SqlStdOperatorTable.TAN)
                    return CallOperator.COT;
                else if (function == SqlStdOperatorTable.COT)
                    return CallOperator.COT;
                else if (function == SqlStdOperatorTable.ACOS)
                    return CallOperator.ACOS;
                else if (function == SqlStdOperatorTable.ASIN)
                    return CallOperator.ASIN;
                else if (function == SqlStdOperatorTable.ATAN)
                    return CallOperator.ATAN;
                else if (function == SqlStdOperatorTable.SQRT)
                    return CallOperator.SQRT;
                else if (function == SqlStdOperatorTable.EXP)
                    return CallOperator.EXP;
                else if (function == SqlStdOperatorTable.LN)
                    return CallOperator.LN;
                else if (function == SqlStdOperatorTable.LOG10)
                    return CallOperator.LOG10;
                else if (function == SqlStdOperatorTable.RAND)
                    return CallOperator.RAND;
                else if (function == SqlStdOperatorTable.ABS)
                    return CallOperator.ABS;
                else if (function == SqlStdOperatorTable.PI)
                    return CallOperator.PI;
                else if (function == SqlStdOperatorTable.SIGN)
                    return CallOperator.SIGN;
                else if (function == SqlStdOperatorTable.ATAN2)
                    return CallOperator.ATAN2;
                else if (function == SqlStdOperatorTable.POWER)
                    return CallOperator.POWER;
                else if (function == SqlStdOperatorTable.DEGREES)
                    return CallOperator.DEGREES;
                else if (function == SqlStdOperatorTable.RADIANS)
                    return CallOperator.RADIANS;
                else if (function == SqlStdOperatorTable.ROUND)
                    return CallOperator.ROUND;
                else if (function == SqlStdOperatorTable.TRUNCATE)
                    return CallOperator.TRUNCATE;

                if (
                    function == SqlStdOperatorTable.CHAR_LENGTH ||
                    function == SqlStdOperatorTable.CHARACTER_LENGTH ||
                    function == HazelcastSqlOperatorTable.LENGTH
                )
                    return CallOperator.CHAR_LENGTH;
                else if (function == SqlStdOperatorTable.UPPER)
                    return CallOperator.UPPER;
                else if (function == SqlStdOperatorTable.LOWER)
                    return CallOperator.LOWER;
                else if (function == SqlStdOperatorTable.INITCAP)
                    return CallOperator.INITCAP;
                else if (function == SqlStdOperatorTable.ASCII)
                    return CallOperator.ASCII;
                else if (function == SqlStdOperatorTable.REPLACE)
                    return CallOperator.REPLACE;
            }

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    private ExpressionConverterRexVisitor() {
        // No-op.
    }
}
