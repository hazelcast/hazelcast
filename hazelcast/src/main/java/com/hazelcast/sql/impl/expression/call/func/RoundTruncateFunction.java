package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.BaseDataType;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Implementation of ROUND/TRUNCATE functions.
 */
public class RoundTruncateFunction<T> extends BiCallExpressionWithType<T> {
    /** Truncate function. */
    private boolean truncate;

    /** Value accessor. */
    private transient Converter valAccessor;

    /** Length accessor. */
    private transient Converter lenAccessor;

    /** Rounding mode. */
    private transient RoundingMode roundingMode;

    public RoundTruncateFunction() {
        // No-op.
    }

    public RoundTruncateFunction(Expression operand1, boolean truncate) {
        this(operand1, null, truncate);
    }

    public RoundTruncateFunction(Expression operand1, Expression operand2, boolean truncate) {
        super(operand1, operand2);

        this.truncate = truncate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        Object val = operand1.eval(ctx, row);

        if (val == null)
            return null;

        if (resType == null) {
            BaseDataType type = operand1.getType().getBaseType();

            switch (type) {
                case BYTE:
                case SHORT:
                case INTEGER:
                    resType = DataType.INT;

                    break;

                case LONG:
                    resType = DataType.BIGINT;

                    break;

                case BIG_INTEGER:
                case BIG_DECIMAL:
                    resType = DataType.DECIMAL;

                    break;

                case FLOAT:
                case DOUBLE:
                    resType = DataType.DOUBLE;

                    break;

                default:
                    throw new HazelcastSqlException(-1, "Unsupported type of the first operand: " + operand1.getType());
            }

            valAccessor = type.getAccessor();

            roundingMode = truncate ? RoundingMode.DOWN : RoundingMode.HALF_UP;
        }

        int len = getLength(ctx, row);

        return (T)(round(val, len));
    }

    /**
     * Perform actual round/truncate.
     *
     * @param val Value.
     * @param len Length.
     * @return Rounded value.
     */
    private Object round(Object val, int len) {
        BigDecimal res = valAccessor.asDecimal(val);

        if (len == 0)
            res = res.setScale(0, roundingMode);
        else
            res = res.movePointRight(len).setScale(0, roundingMode).movePointLeft(len);

        try {
            switch (resType.getBaseType()) {
                case INTEGER:
                    return res.intValueExact();

                case LONG:
                    return res.longValueExact();

                case BIG_DECIMAL:
                    return res;

                case DOUBLE:
                    return res.doubleValue();

                default:
                    throw new HazelcastSqlException(-1, "Unexpected result type: " + resType);
            }
        }
        catch (ArithmeticException e) {
            throw new HazelcastSqlException(-1, "Data overflow.");
        }
    }

    /**
     * Get length (second operand).
     *
     * @param ctx Context.
     * @param row Row.
     * @return Length.
     */
    private int getLength(QueryContext ctx, Row row) {
        Object len = operand2 != null ? operand2.eval(ctx, row) : null;

        if (len == null)
            return 0;

        if (lenAccessor == null) {
            BaseDataType type = operand2.getType().getBaseType();

            switch (type) {
                case BYTE:
                case SHORT:
                case INTEGER:
                    lenAccessor = type.getAccessor();

                    break;

                default:
                    throw new HazelcastSqlException(-1, "Unsupported type of the second operand: " +
                        operand2.getType());
            }
        }

        return lenAccessor.asInt(len);
    }

    @Override
    public int operator() {
        return truncate ? CallOperator.TRUNCATE : CallOperator.ROUND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(truncate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        truncate = in.readBoolean();
    }
}
