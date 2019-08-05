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
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

import java.math.BigDecimal;

/**
 * Remainder expression.
 */
public class RemainderFunction<T> extends BiCallExpressionWithType<T> {
    /** Accessor for the first argument. */
    private transient BaseDataTypeAccessor accessor1;

    /** Accessor for the second argument. */
    private transient BaseDataTypeAccessor accessor2;

    public RemainderFunction() {
        // No-op.
    }

    public RemainderFunction(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    // TODO: Duplicates remainder.
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

            resType = TypeUtils.inferForDivideRemainder(type1, type2);

            accessor1 = type1.getBaseType().getAccessor();
            accessor2 = type2.getBaseType().getAccessor();
        }

        // Execute.
        return (T)doDivide(op1, op2, accessor1, accessor2, resType);
    }

    @SuppressWarnings("unchecked")
    private static Object doDivide(
        Object op1,
        Object op2,
        BaseDataTypeAccessor accessor1,
        BaseDataTypeAccessor accessor2,
        DataType resType
    ) {
        try {
            switch (resType.getBaseType()) {
                case BYTE:
                    return (byte)(accessor1.getByte(op1) % accessor2.getByte(op2));

                case SHORT:
                    return (short)(accessor1.getShort(op1) % accessor2.getShort(op2));

                case INTEGER:
                    return accessor1.getInt(op1) % accessor2.getInt(op2);

                case LONG:
                    return accessor1.getLong(op1) % accessor2.getLong(op2);

                case BIG_DECIMAL:
                    BigDecimal op1Decimal = accessor1.getDecimal(op1);
                    BigDecimal op2Decimal = accessor2.getDecimal(op2);

                    return op1Decimal.remainder(op2Decimal);

                case FLOAT: {
                    float res = accessor1.getFloat(op1) % accessor2.getFloat(op2);

                    if (Float.isInfinite(res))
                        throw new HazelcastSqlException(-1, "Division by zero.");

                    return res;
                }

                case DOUBLE: {
                    double res = accessor1.getDouble(op1) % accessor2.getDouble(op2);

                    if (Double.isInfinite(res))
                        throw new HazelcastSqlException(-1, "Division by zero.");

                    return res;
                }

                default:
                    throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
            }
        }
        catch (ArithmeticException e) {
            throw new HazelcastSqlException(-1, "Division by zero.");
        }
    }

    @Override public int operator() {
        return CallOperator.REMAINDER;
    }
}
