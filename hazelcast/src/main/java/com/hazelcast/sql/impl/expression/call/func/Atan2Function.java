package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.BiCallExpression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.TypeUtils;
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

public class Atan2Function extends BiCallExpression<Double> {
    /** Accessor for the first argument. */
    private transient BaseDataTypeAccessor accessor1;

    /** Accessor for the second argument. */
    private transient BaseDataTypeAccessor accessor2;

    public Atan2Function() {
        // No-op.
    }

    public Atan2Function(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @Override
    public Double eval(QueryContext ctx, Row row) {
        Object op1 = operand1.eval(ctx, row);

        if (op1 == null)
            return null;
        else if (accessor1 == null)
            accessor1 = TypeUtils.numericAccessor(operand1, 1);

        Object op2 = operand2.eval(ctx, row);

        if (op2 == null)
            return null;
        else if (accessor1 == null)
            accessor2 = TypeUtils.numericAccessor(operand2, 2);

        return Math.atan2(accessor1.getDouble(op1), accessor2.getDouble(op2));
    }

    @Override
    public DataType getType() {
        return DataType.DOUBLE;
    }

    @Override
    public int operator() {
        return CallOperator.ATAN2;
    }
}
