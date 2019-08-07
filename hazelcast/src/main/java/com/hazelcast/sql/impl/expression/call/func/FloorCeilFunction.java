package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
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

    /** Operand type. */
    private transient DataType operandType;

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
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null)
            return null;

        if (resType == null) {
            DataType type = operand.getType();

            if (!type.isCanConvertToNumeric())
                throw new HazelcastSqlException(-1, "Operand is not numeric: " + type);

            switch (type.getType()) {
                case BIT:
                    resType = DataType.TINYINT;

                    break;

                case REAL:
                    resType = DataType.DOUBLE;

                    break;

                default:
                    resType = type;
            }

            operandType = type;
        }

        return (T)floorCeil(operandValue, operandType, resType, ceil);
    }

    private static Object floorCeil(Object operand, DataType operandType, DataType resType, boolean ceil) {
        Converter operandConverter = operandType.getConverter();

        switch (resType.getType()) {
            case TINYINT:
                return operandConverter.asTinyInt(operand);

            case SMALLINT:
                return operandConverter.asSmallInt(operand);

            case INT:
                return operandConverter.asInt(operand);

            case BIGINT:
                return operandConverter.asBigInt(operand);

            case DECIMAL: {
                BigDecimal operand0 = operandConverter.asDecimal(operand);

                RoundingMode roundingMode = ceil ? RoundingMode.CEILING : RoundingMode.FLOOR;

                return operand0.setScale(0, roundingMode);
            }

            case DOUBLE: {
                double operand0 = operandConverter.asDouble(operand);

                return ceil ? Math.ceil(operand0) : Math.floor(operand0);
            }
        }

        throw new HazelcastSqlException(-1, "Unexpected type: " + resType);
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
