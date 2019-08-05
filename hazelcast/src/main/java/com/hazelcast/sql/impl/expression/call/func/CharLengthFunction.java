package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

public class CharLengthFunction extends UniCallExpression<Integer> {
    /** Accessor. */
    private transient BaseDataTypeAccessor accessor;

    public CharLengthFunction() {
        // No-op.
    }

    public CharLengthFunction(Expression operand) {
        super(operand);
    }

    @Override
    public Integer eval(QueryContext ctx, Row row) {
        Object op = operand.eval(ctx, row);

        if (op == null)
            return null;

        if (accessor == null)
            accessor = operand.getType().getBaseType().getAccessor();

        return accessor.getString(op).length();
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
