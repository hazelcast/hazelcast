package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.BaseDataType;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Implementation of FLOOR/CEIL function.
 */
public class FloorCeilFunction<T> extends UniCallExpressionWithType<T> {
    /** If this is the CEIL call. */
    private boolean ceil;

    public FloorCeilFunction() {
        // No-op.
    }

    public FloorCeilFunction(Expression operand, boolean ceil) {
        super(operand);

        this.ceil = ceil;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        Object op = operand.eval(ctx, row);

        if (op == null)
            return null;

        if (resType == null) {
            DataType opType = operand.getType();

            if (!opType.isNumeric())
                throw new HazelcastSqlException(-1, "Operand is not numeric: " + opType);

            switch (opType.getBaseType()) {
                case BOOLEAN:
                    throw new HazelcastSqlException(-1, "BIT type is not supported for FLOOR/CEIL operations.");

                case BIG_INTEGER:
                    resType = DataType.DECIMAL_INTEGER_DECIMAL;

                    break;

                case FLOAT:
                    resType = DataType.DOUBLE;

                default:
                    resType = opType;
            }
        }

        return (T)floorCeil(op, resType.getBaseType(), ceil);
    }

    private static Object floorCeil(Object operand, BaseDataType type, boolean ceil) {
        Converter accessor = type.getAccessor();

        switch (type) {
            case BYTE:
                return accessor.asTinyInt(operand);

            case SHORT:
                return accessor.asSmallInt(operand);

            case INTEGER:
                return accessor.asInt(operand);

            case LONG:
                return accessor.asBigInt(operand);

            case BIG_DECIMAL: {
                BigDecimal operand0 = accessor.asDecimal(operand);

                RoundingMode roundingMode = ceil ? RoundingMode.CEILING : RoundingMode.FLOOR;

                return operand0.setScale(0, roundingMode);
            }

            case DOUBLE: {
                double operand0 = accessor.asDouble(operand);

                return ceil ? Math.ceil(operand0) : Math.floor(operand0);
            }
        }

        throw new HazelcastSqlException(-1, "Unexpected type: " + type);
    }

    @Override
    public int operator() {
        return ceil ? CallOperator.CEIL : CallOperator.FLOOR;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(ceil);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        ceil = in.readBoolean();
    }
}
