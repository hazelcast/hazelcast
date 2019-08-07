package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.BiCallExpression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

/**
 * A function which accepts a string, and return another string.
 */
public class ConcatFunction extends BiCallExpression<String> {
    /** Accessor of operand 1. */
    private transient Converter accessor1;

    /** Accessor of operand 2. */
    private transient Converter accessor2;

    public ConcatFunction() {
        // No-op.
    }

    public ConcatFunction(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @Override
    public String eval(QueryContext ctx, Row row) {
        Object op1 = operand1.eval(ctx, row);
        Object op2 = operand2.eval(ctx, row);

        if (op1 != null && accessor1 == null)
            accessor1 = operand1.getType().getBaseType().getAccessor();

        if (op2 != null && accessor2 == null)
            accessor2 = operand2.getType().getBaseType().getAccessor();

        if (op1 == null) {
            if (op2 == null)
                return "";
            else
                return accessor2.asVarchar(op2);
        }
        else {
            if (op2 == null)
                return accessor1.asVarchar(op1);
            else
                return accessor1.asVarchar(op1) + accessor2.asVarchar(op2);
        }
    }

    @Override
    public DataType getType() {
        return DataType.VARCHAR;
    }

    @Override
    public int operator() {
        return CallOperator.CONCAT;
    }
}
