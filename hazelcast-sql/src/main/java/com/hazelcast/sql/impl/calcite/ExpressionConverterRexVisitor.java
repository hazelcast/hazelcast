package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.AbsFunction;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.DoubleFunction;
import com.hazelcast.sql.impl.expression.call.MinusFunction;
import com.hazelcast.sql.impl.expression.call.MultiplyFunction;
import com.hazelcast.sql.impl.expression.call.PlusFunction;
import com.hazelcast.sql.impl.expression.call.RandomFunction;
import com.hazelcast.sql.impl.expression.call.UnaryMinusFunction;
import com.hazelcast.sql.impl.expression.call.func.CharLengthFunction;
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

        int convertedOperator = convertOperator(operator);

        // Convert operands.
        List<RexNode> operands = call.getOperands();

        List<Expression> convertedOperands;

        if (operands == null || operands.isEmpty())
            convertedOperands = Collections.emptyList();
        else {
            convertedOperands = new ArrayList<>(operands.size());

            for (RexNode operand : operands) {
                Expression convertedOperand = operand.accept(this);

                convertedOperands.add(convertedOperand);
            }
        }

        switch (convertedOperator) {
            case CallOperator.PLUS:
                return new PlusFunction(convertedOperands.get(0), convertedOperands.get(1));

            case CallOperator.MINUS:
                return new MinusFunction(convertedOperands.get(0), convertedOperands.get(1));

            case CallOperator.MULTIPLY:
                return new MultiplyFunction(convertedOperands.get(0), convertedOperands.get(1));

            case CallOperator.UNARY_MINUS:
                return new UnaryMinusFunction(convertedOperands.get(0));

            case CallOperator.CHAR_LENGTH:
                return new CharLengthFunction(convertedOperands.get(0));

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
                return new DoubleFunction(convertedOperands.get(0), convertedOperator);

            case CallOperator.RAND:
                if (convertedOperands.isEmpty())
                    return new RandomFunction();
                else {
                    assert convertedOperands.size() == 1;

                    return new RandomFunction(convertedOperands.get(0));
                }

            case CallOperator.ABS:
                return new AbsFunction(convertedOperands.get(0));

            case CallOperator.PI:
                return new ConstantExpression<>(Math.PI);

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

            case MINUS_PREFIX:
                return CallOperator.UNARY_MINUS;

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

                if (
                    function == SqlStdOperatorTable.CHAR_LENGTH ||
                    function == SqlStdOperatorTable.CHARACTER_LENGTH ||
                    function == HazelcastSqlOperatorTable.LENGTH
                )
                    return CallOperator.CHAR_LENGTH;
            }

            default:
                // TODO: Proper exception.
                throw new UnsupportedOperationException("Unsupported operator: " + operator);
        }
    }

    private ExpressionConverterRexVisitor() {
        // No-op.
    }
}
