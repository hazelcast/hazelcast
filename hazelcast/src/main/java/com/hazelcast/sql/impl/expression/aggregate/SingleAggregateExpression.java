package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

/**
 * Aggregate accumulator which uses only a single input.
 */
public abstract class SingleAggregateExpression<T> extends AggregateExpression<T> {
    /** Operand. */
    private Expression operand;

    /** Operand type. */
    protected transient DataType operandType;

    public SingleAggregateExpression() {
        // No-op.
    }

    public SingleAggregateExpression(boolean distinct, Expression operand) {
        super(distinct);

        this.operand = operand;
    }

    @Override
    public void collect(QueryContext ctx, Row row, AggregateCollector collector) {
        Object operandValue = operand.eval(ctx, row);

        // Null operands are not processed.
        if (operandValue == null)
            return;

        // Resolve types.
        if (operandType == null) {
            operandType = operand.getType();

            resType = resolveReturnType(operandType);
        }

        collector.collect(operandValue);
    }

    /**
     * Resolve return type for the accumulator.
     *
     * @param operandType Operand type.
     * @return Return type.
     */
    protected abstract DataType resolveReturnType(DataType operandType);
}
