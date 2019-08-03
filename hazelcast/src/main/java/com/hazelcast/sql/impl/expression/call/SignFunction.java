package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.BaseDataType;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

public class SignFunction extends UniCallExpressionWithType<Number> {
    public SignFunction() {
        // No-op.
    }

    public SignFunction(Expression operand) {
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

            switch (operandType.getBaseType()) {
                case BOOLEAN:
                    throw new HazelcastSqlException(-1, "SIGN operation is not supported for BIT data type.");

                case BYTE:
                case SHORT:
                case INTEGER:
                    resType = DataType.INT;

                    break;

                case BIG_DECIMAL:
                    resType = DataType.DECIMAL_INTEGER_DECIMAL;

                    break;

                default:
                    resType = operandType;
            }
        }

        return sign(val, resType.getBaseType());
    }

    /**
     * Get absolute value.
     *
     * @param val Value.
     * @param type Type.
     * @return Absolute value of the target.
     */
    private static Number sign(Object val, BaseDataType type) {
        BaseDataTypeAccessor accessor = type.getAccessor();

        switch (type) {
            case INTEGER:
                return Integer.signum(accessor.getInt(val));

            case LONG:
                return Long.signum(accessor.getLong(val));

            case BIG_DECIMAL:
                return accessor.getDecimal(val).signum();

            case FLOAT:
                return Math.signum(accessor.getFloat(val));

            case DOUBLE:
                return Math.signum(accessor.getDouble(val));
        }

        throw new HazelcastSqlException(-1, "Unexpected type: " + type);
    }

    @Override
    public int operator() {
        return CallOperator.SIGN;
    }
}
