package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.TypeUtils;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;

/**
 * Unary minus operation.
 */
public class UnaryMinusFunction<T> extends UniCallExpressionWithType<T> {
    /** Accessor for the argument. */
    private transient Converter accessor;

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
    private static Object doMinus(Object op, Converter accessor, DataType resType) {
        switch (resType.getBaseType()) {
            case BYTE:
                return (byte)(-accessor.asTinyInt(op));

            case SHORT:
                return (short)(-accessor.asSmallInt(op));

            case INTEGER:
                return -accessor.asInt(op);

            case LONG:
                return -accessor.asBigInt(op);

            case BIG_DECIMAL:
                BigDecimal opDecimal = accessor.asDecimal(op);

                return opDecimal.negate();

            case FLOAT:
                return -accessor.asReal(op);

            case DOUBLE:
                return -accessor.asDouble(op);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    @Override
    public int operator() {
        return CallOperator.UNARY_MINUS;
    }
}
