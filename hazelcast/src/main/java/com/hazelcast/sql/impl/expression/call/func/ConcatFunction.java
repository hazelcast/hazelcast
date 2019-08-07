package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.BiCallExpression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

/**
 * A function which accepts a string, and return another string.
 */
public class ConcatFunction extends BiCallExpression<String> {
    /** Type of operand 1. */
    private transient DataType operand1Type;

    /** Type of operand 2. */
    private transient DataType operand2Type;

    public ConcatFunction() {
        // No-op.
    }

    public ConcatFunction(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @Override
    public String eval(QueryContext ctx, Row row) {
        Object operand1Value = operand1.eval(ctx, row);
        Object operand2Value = operand2.eval(ctx, row);

        if (operand1Value != null && operand1Type == null)
            operand1Type = operand1.getType();

        if (operand2Value != null && operand2Type == null)
            operand2Type = operand2.getType();

        if (operand1Value == null) {
            if (operand2Value == null)
                return "";
            else
                return operand2Type.getConverter().asVarchar(operand2Value);
        }
        else {
            if (operand2Value == null)
                return operand1Type.getConverter().asVarchar(operand1Value);
            else {
                return operand1Type.getConverter().asVarchar(operand1Value) +
                    operand2Type.getConverter().asVarchar(operand2Value);
            }
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
