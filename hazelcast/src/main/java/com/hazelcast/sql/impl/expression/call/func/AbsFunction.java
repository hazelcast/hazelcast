package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.BaseDataType;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

public class AbsFunction extends UniCallExpressionWithType<Number> {
    public AbsFunction() {
        // No-op.
    }

    public AbsFunction(Expression operand) {
        super(operand);
    }

    @Override
    public Number eval(QueryContext ctx, Row row) {
        Object val = operand.eval(ctx, row);

        if (val == null)
            return null;

        if (resType == null) {
            DataType operandType = operand.getType();

            if (!operandType.isNumeric())
                throw new HazelcastSqlException(-1, "Operand is not numeric: " + val);

            if (operandType.getBaseType() == BaseDataType.BIG_INTEGER)
                resType = DataType.DECIMAL_INTEGER_DECIMAL;
            else
                resType = operandType;
        }

        return abs(val, resType.getBaseType());
    }

    /**
     * Get absolute value.
     *
     * @param val Value.
     * @param type Type.
     * @return Absolute value of the target.
     */
    private static Number abs(Object val, BaseDataType type) {
        BaseDataTypeAccessor accessor = type.getAccessor();

        switch (type) {
            case BYTE:
                return (byte)Math.abs(accessor.getByte(val));

            case SHORT:
                return (short)Math.abs(accessor.getShort(val));

            case INTEGER:
                return Math.abs(accessor.getInt(val));

            case LONG:
                return Math.abs(accessor.getLong(val));

            case BIG_DECIMAL:
                return accessor.getDecimal(val).abs();

            case FLOAT:
                return Math.abs(accessor.getFloat(val));

            case DOUBLE:
                return Math.abs(accessor.getDouble(val));
        }

        throw new HazelcastSqlException(-1, "Unexpected type: " + type);
    }

    @Override
    public int operator() {
        return CallOperator.ABS;
    }
}
