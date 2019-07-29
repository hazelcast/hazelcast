package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;

public class CharLengthFunction extends UniCallExpression<Integer> {

    public CharLengthFunction() {
        // No-op.
    }

    public CharLengthFunction(Expression operand) {
        super(operand);
    }

    @Override
    public Integer eval(QueryContext ctx, Row row) {
        Object op1 = getOperand().eval(ctx, row);

        if (op1 == null) {
            // TODO: How is this handled?
            return null;
        }

        if (!(op1 instanceof String))
            // TODO: Proper SQLSTATE
            throw new IllegalArgumentException("Illegal type: " + op1.getClass());

        return ((String)op1).length();
    }

    @Override
    public int operator() {
        return CallOperator.CHAR_LENGTH;
    }
}
