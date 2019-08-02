package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.TypeUtils;
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

import java.math.BigDecimal;

/**
 * Unary minus operation.
 */
public class UnaryMinusFunction<T> extends UniCallExpression<T> {
    /** Accessor for the argument. */
    private transient BaseDataTypeAccessor accessor;

    public UnaryMinusFunction() {
        // No-op.
    }

    public UnaryMinusFunction(Expression operand) {
        super(operand);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        Object op = operand.eval(ctx, row);

        if (op == null)
            return null;

        if (resType == null) {
            DataType type = operand.getType();

            resType = TypeUtils.inferForUnaryMinus(type);

            accessor = type.getBaseType().getAccessor();
        }

        return (T)doMinus(op, accessor, resType);
    }

    @SuppressWarnings("unchecked")
    private static Object doMinus(Object op, BaseDataTypeAccessor accessor, DataType resType) {
        switch (resType.getBaseType()) {
            case BYTE:
                return (byte)(-accessor.getByte(op));

            case SHORT:
                return (short)(-accessor.getShort(op));

            case INTEGER:
                return -accessor.getInt(op);

            case LONG:
                return -accessor.getLong(op);

            case BIG_DECIMAL:
                BigDecimal opDecimal = accessor.getDecimal(op);

                return opDecimal.negate();

            case FLOAT:
                return -accessor.getFloat(op);

            case DOUBLE:
                return -accessor.getDouble(op);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    @Override
    public int operator() {
        return CallOperator.UNARY_MINUS;
    }
}
