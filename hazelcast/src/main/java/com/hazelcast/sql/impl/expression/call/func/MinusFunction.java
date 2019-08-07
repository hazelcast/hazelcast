package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.TypeUtils;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;

public class MinusFunction<T> extends BiCallExpressionWithType<T> {
    /** Accessor for the first argument. */
    private transient Converter accessor1;

    /** Accessor for the second argument. */
    private transient Converter accessor2;

    public MinusFunction() {
        // No-op.
    }

    public MinusFunction(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    // TODO: Remove duplication (merge with plus?)
    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        // Calculate child operands with fail-fast NULL semantics.
        Object op1 = operand1.eval(ctx, row);

        if (op1 == null)
            return null;

        Object op2 = operand2.eval(ctx, row);

        if (op2 == null)
            return null;

        // Prepare result type if needed.
        if (resType == null) {
            DataType type1 = operand1.getType();
            DataType type2 = operand2.getType();

            resType = TypeUtils.inferForPlusMinus(type1, type2);

            accessor1 = type1.getBaseType().getAccessor();
            accessor2 = type2.getBaseType().getAccessor();
        }

        // Execute.
        return (T)doMinus(op1, op2, accessor1, accessor2, resType);
    }

    @SuppressWarnings("unchecked")
    private static Object doMinus(Object op1, Object op2, Converter accessor1, Converter accessor2,
        DataType resType) {
        switch (resType.getBaseType()) {
            case BYTE:
                return accessor1.asTinyInt(op1) - accessor2.asTinyInt(op2);

            case SHORT:
                return accessor1.asSmallInt(op1) - accessor2.asSmallInt(op2);

            case INTEGER:
                return accessor1.asInt(op1) - accessor2.asInt(op2);

            case LONG:
                return accessor1.asBigInt(op1) - accessor2.asBigInt(op2);

            case BIG_DECIMAL:
                BigDecimal op1Decimal = accessor1.asDecimal(op1);
                BigDecimal op2Decimal = accessor2.asDecimal(op2);

                return op1Decimal.subtract(op2Decimal);

            case FLOAT:
                return accessor1.asReal(op1) - accessor2.asReal(op2);

            case DOUBLE:
                return accessor1.asDouble(op1) - accessor2.asDouble(op2);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    @Override
    public int operator() {
        return CallOperator.MINUS;
    }
}
