package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.exec.agg.LocalAggregateExec;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 * Aggregate accumulator which uses only a single input.
 */
public abstract class SingleAggregateExpression<T> extends AggregateExpression<T> {
    /** Operand. */
    private Expression operand;

    /** Operand type. */
    protected transient DataType operandType;

    /** Distinct operands. */
    // TODO: Move to collector! Expression should be stateless.
    private transient Set<Object> distinctSet;

    public SingleAggregateExpression() {
        // No-op.
    }

    public SingleAggregateExpression(boolean distinct, Expression operand) {
        super(distinct);

        this.operand = operand;
    }

    @Override
    public void setup(LocalAggregateExec parent) {
        super.setup(parent);

        if (distinct)
            distinctSet = new HashSet<>();
    }

    @Override
    public void collect(QueryContext ctx, Row row, AggregateCollector collector) {
        Object operandValue = operand.eval(ctx, row);

        // Null operands are not processed.
        if (operandValue == null)
            return;

        // Check for uniqueness.
        if (distinct && !isDistinct(operandValue))
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

    /**
     * Check if the given value is unique for this accumulator.
     *
     * @param operandValue Value.
     * @return {@code True} if this is the first occurrence of the value.
     */
    private boolean isDistinct(Object operandValue) {
        return distinctSet.add(operandValue);
    }
}
