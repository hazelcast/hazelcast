package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.type.DataType;

/**
 * Counting accumulator.
 */
public class CountAggregateAccumulator extends SingleAggregateAccumulator<Long> {
    /** Final result. */
    private transient long res;

    @Override
    protected void collect(Object value) {
        res++;
    }

    @Override
    protected Long reduceAndReset() {
        long res0 = res;

        res = 0;

        return res0;
    }

    @Override
    protected DataType resolveReturnType(DataType operandType) {
        return DataType.BIGINT;
    }
}
