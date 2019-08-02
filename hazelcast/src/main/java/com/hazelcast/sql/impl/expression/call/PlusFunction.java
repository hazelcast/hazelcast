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
 * Plus expression.
 */
public class PlusFunction<T> extends BiCallExpression<T> {
    /** Accessor for the first argument. */
    private transient BaseDataTypeAccessor accessor1;

    /** Accessor for the second argument. */
    private transient BaseDataTypeAccessor accessor2;

    public PlusFunction() {
        // No-op.
    }

    public PlusFunction(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

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
        return (T)doSum(op1, op2, accessor1, accessor2, resType);
    }

    @SuppressWarnings("unchecked")
    private static Object doSum(Object op1, Object op2, BaseDataTypeAccessor accessor1, BaseDataTypeAccessor accessor2,
        DataType resType) {
        switch (resType.getBaseType()) {
            case BYTE:
                return accessor1.getByte(op1) + accessor2.getByte(op2);

            case SHORT:
                return accessor1.getShort(op1) + accessor2.getShort(op2);

            case INTEGER:
                return accessor1.getInt(op1) + accessor2.getInt(op2);

            case LONG:
                return accessor1.getLong(op1) + accessor2.getLong(op2);

            case BIG_DECIMAL:
                BigDecimal op1Decimal = accessor1.getDecimal(op1);
                BigDecimal op2Decimal = accessor2.getDecimal(op2);

                return op1Decimal.add(op2Decimal);

            case FLOAT:
                return accessor1.getFloat(op1) + accessor2.getFloat(op2);

            case DOUBLE:
                return accessor1.getDouble(op1) + accessor2.getDouble(op2);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    @Override public int operator() {
        return CallOperator.PLUS;
    }
}
