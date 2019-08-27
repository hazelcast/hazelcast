package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;

/**
 * Counting accumulator.
 */
public class CountAggregateExpression extends SingleAggregateExpression<Long> {
    public CountAggregateExpression() {
        // No-op.
    }

    public CountAggregateExpression(boolean distinct, Expression operand) {
        super(distinct, operand);
    }

    @Override
    public AggregateCollector newCollector(QueryContext ctx) {
        return new Collector(distinct);
    }

    @Override
    protected DataType resolveReturnType(DataType operandType) {
        return DataType.BIGINT;
    }

    /**
     * Counting collector.
     */
    private static class Collector extends AggregateCollector {
        /** Final result. */
        private long res;

        public Collector(boolean distinct) {
            super(distinct);
        }

        @Override
        protected void collect0(Object value) {
            res++;
        }

        @Override
        public Object reduce() {
            return res;
        }

        @Override
        public void reset() {
            res = 0;
        }
    }
}
