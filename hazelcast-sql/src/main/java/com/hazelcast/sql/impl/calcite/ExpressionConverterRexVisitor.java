package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.expression.CallExpression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
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
import org.apache.calcite.sql.SqlOperator;

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

        return new CallExpression(convertedOperator, convertedOperands);
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

            default:
                // TODO: Proper exception.
                throw new UnsupportedOperationException("Unsupported operator: " + operator);
        }
    }

    private ExpressionConverterRexVisitor() {
        // No-op.
    }
}
