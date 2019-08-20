package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.GenericType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;

/**
 * Summing accumulator.
 */
public class SumAggregateAccumulator<T> extends SingleAggregateAccumulator<T> {
    /** Result. */
    private transient Object res;

    @Override
    protected void collect(Object value) {
        Converter converter = operandType.getConverter();

        switch (resType.getType()) {
            case INT:
                res = (int)res + converter.asInt(value);

                break;

            case BIGINT:
                res = (long)res + converter.asBigInt(value);

                break;

            case DECIMAL:
                res = ((BigDecimal)res).add(converter.asDecimal(value));

                break;

            default:
                assert resType.getType() == GenericType.DOUBLE;

                res = (double)res + converter.asDouble(value);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T reduceAndReset() {
        Object res0 = res;

        reset();

        return (T)res0;
    }

    /**
     * Reset value to default.
     */
    private void reset() {
        switch (resType.getType()) {
            case INT:
                res = 0;

                break;

            case BIGINT:
                res = 0;

                break;

            case DECIMAL:
                res = BigDecimal.ZERO;

                break;

            default:
                assert resType.getType() == GenericType.DOUBLE;

                res = 0.0d;
        }
    }

    @Override
    protected DataType resolveReturnType(DataType operandType) {
        switch (operandType.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
                res = 0;

                return DataType.INT;

            case BIGINT:
                res = 0L;

                return DataType.BIGINT;

            case DECIMAL:
                res = BigDecimal.ZERO;

                return DataType.DECIMAL;

            case REAL:
            case DOUBLE:
                res = 0.0d;

                return DataType.DOUBLE;

            default:
                throw new HazelcastSqlException(-1, "Unsupported operand type: " + operandType);
        }
    }
}
