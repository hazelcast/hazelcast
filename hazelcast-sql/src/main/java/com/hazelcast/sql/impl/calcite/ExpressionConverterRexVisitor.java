package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.MinusBiCallExpression;
import com.hazelcast.sql.impl.expression.call.MinusUniCallExpression;
import com.hazelcast.sql.impl.expression.call.PlusBiCallExpression;
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
        // TODO
        throw new UnsupportedOperationException();
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
                return new PlusBiCallExpression(convertedOperands.get(0), convertedOperands.get(1));

            case CallOperator.MINUS:
                return new MinusBiCallExpression(convertedOperands.get(0), convertedOperands.get(1));

            case CallOperator.UNARY_MINUS:
                return new MinusUniCallExpression(convertedOperands.get(0));

            case CallOperator.CHAR_LENGTH:
                return new CharLengthFunction(convertedOperands.get(0));

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

            case MINUS_PREFIX:
                return CallOperator.UNARY_MINUS;

            case OTHER_FUNCTION: {
                SqlFunction function = (SqlFunction)operator;

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
