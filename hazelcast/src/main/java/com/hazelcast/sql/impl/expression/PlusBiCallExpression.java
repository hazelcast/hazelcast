package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.optimized.PlusBiOpt;
import com.hazelcast.sql.impl.metadata.ExpressionType;
import com.hazelcast.sql.impl.row.Row;

/**
 * Plus expression.
 */
public class PlusBiCallExpression<T> extends BiCallExpression<T> {
    /** Expected type of the first operand. */
    private transient ExpressionType type1;

    /** Expected type of the second operand. */
    private transient ExpressionType type2;

    private transient PlusBiOpt<T> opt;

    public PlusBiCallExpression() {
        // No-op.
    }

    public PlusBiCallExpression(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @Override
    public Object eval(QueryContext ctx, Row row) {
        Object res1 = getOperand1().eval(ctx, row);
        Object res2 = getOperand1().eval(ctx, row);

        if (res1 == null || res2 == null)
            return null;

        prepareTypes(res1, res2);

        return opt.eval(res1, res2);
    }

    /**
     * Prepare types of operands.
     *
     * @param res1 Result 1.
     * @param res2 Result 2.
     */
    private void prepareTypes(Object res1, Object res2) {
        // Fast-path for the same metadata.
        if (type1 != null && type1.isSame(res1) && type2 != null && type2.isSame(res2))
            return;

        boolean optimize = false;

        if (type1 == null || !type1.isSame(res1)) {
            type1 = ExpressionType.of(res1);

            if (!type1.isNumeric())
                // TODO: Proper exception.
                throw new IllegalArgumentException("First operand is not numeric: " + res1);

            optimize = true;
        }

        if (type2 == null || !type2.isSame(res2)) {
            type2 = ExpressionType.of(res2);

            if (!type2.isNumeric())
                // TODO: Proper exception.
                throw new IllegalArgumentException("Second operand is not numeric: " + res1);

            optimize = true;
        }

        if (optimize)
            opt = PlusBiOpt.forTypes(type1, type2);
    }

    @Override public int operator() {
        return CallOperator.PLUS;
    }
}
