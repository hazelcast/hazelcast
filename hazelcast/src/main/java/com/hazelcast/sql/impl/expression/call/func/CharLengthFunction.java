package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

public class CharLengthFunction extends UniCallExpression<Integer> {

    public CharLengthFunction() {
        // No-op.
    }

    public CharLengthFunction(Expression operand) {
        super(operand);
    }

    @Override
    public Integer eval(QueryContext ctx, Row row) {
        Object op1 = operand.eval(ctx, row);

        if (op1 == null) {
            // TODO: How is this handled? See spec.
            return null;
        }

        if (operand.getType() != DataType.VARCHAR)
            // TODO: Proper SQLSTATE
            throw new HazelcastSqlException(-1, "Illegal type: " + op1.getClass());

        return ((String)op1).length();
    }

    @Override
    public DataType getType() {
        return DataType.INT;
    }

    @Override
    public int operator() {
        return CallOperator.CHAR_LENGTH;
    }
}
