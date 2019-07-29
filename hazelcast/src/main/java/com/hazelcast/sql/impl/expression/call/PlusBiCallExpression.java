package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.metadata.ExpressionType;
import com.hazelcast.sql.impl.row.Row;

import java.math.BigDecimal;

/**
 * Plus expression.
 */
public class PlusBiCallExpression<T> extends BiCallExpression<T> {
    /** Expected type of the first operand. */
    private transient ExpressionType expType1 = ExpressionType.UNKNOWN;

    /** Expected type of the second operand. */
    private transient ExpressionType expType2 = ExpressionType.UNKNOWN;

    /** Whether operands should be switched for proper type alignment. */
    private transient boolean switchOperands;

    /** Maximum precedence which defines execution mode. */
    private transient int maxPrecedence;

    public PlusBiCallExpression() {
        // No-op.
    }

    public PlusBiCallExpression(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @Override
    public T eval(QueryContext ctx, Row row) {
        Object res1 = getOperand1().eval(ctx, row);
        Object res2 = getOperand2().eval(ctx, row);

        if (res1 == null || res2 == null)
            return null;

        changeTypeIfNeeded(res1, res2);

        return eval0(res1, res2);
    }

    @SuppressWarnings("unchecked")
    private T eval0(Object op1, Object op2) {
        if (switchOperands) {
            Object tmp = op1;
            op1 = op2;
            op2 = tmp;
        }

        Object res;

        switch (maxPrecedence) {
            case ExpressionType.PRECEDENCE_TINYINT:
                res = ((Byte)op1).intValue() + (Byte)op2;

                break;

            case ExpressionType.PRECEDENCE_SMALLINT:
                res = ((Short)op1).intValue() + (Short)op2;

                break;

            case ExpressionType.PRECEDENCE_INT:
                res =((Integer)op1).longValue() + (Integer)op2;

                break;

            case ExpressionType.PRECEDENCE_BIGINT:
                res = new BigDecimal((Long)op1).add(new BigDecimal((Long)op2));

                break;

            case ExpressionType.PRECEDENCE_DECIMAL: {
                BigDecimal op1Converted = (BigDecimal)op1;
                BigDecimal op2Coverted = op2 instanceof BigDecimal ? (BigDecimal)op2 : new BigDecimal((Long)op2);

                res = op1Converted.add(op2Coverted);

                break;
            }

            case ExpressionType.PRECEDENCE_REAL:
            case ExpressionType.PRECEDENCE_DOUBLE:
                res = ((Number)op1).doubleValue() + ((Number)op2).doubleValue();

                break;

            default:
                throw new IllegalStateException("Should never happen.");
        }

        return (T)res;
    }

    /**
     * Prepare types of operands.
     *
     * @param op1 Result 1.
     * @param op2 Result 2.
     */
    private void changeTypeIfNeeded(Object op1, Object op2) {
        // Fast-path for the same metadata.
        boolean same1 = expType1.isSame(op1);
        boolean same2 = expType2.isSame(op2);

        if (same1 && same2)
            return;

        if (!same1) {
            expType1 = ExpressionType.of(op1);

            if (!expType1.isNumeric())
                // TODO: Proper exception.
                throw new IllegalArgumentException("First operand is not numeric: " + op1);
        }

        if (!same2) {
            expType2 = ExpressionType.of(op2);

            if (!expType2.isNumeric())
                // TODO: Proper exception.
                throw new IllegalArgumentException("Second operand is not numeric: " + op1);
        }

        if (expType1.precedence() >= expType2.precedence()) {
            maxPrecedence = expType1.precedence();

            switchOperands = false;
        }
        else {
            maxPrecedence = expType2.precedence();

            switchOperands = true;
        }
    }

    @Override public int operator() {
        return CallOperator.PLUS;
    }
}
