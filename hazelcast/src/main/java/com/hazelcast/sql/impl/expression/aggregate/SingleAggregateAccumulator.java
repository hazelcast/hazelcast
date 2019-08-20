package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.AggregateExec;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 * Aggregate accumulator which uses only a single input.
 */
public abstract class SingleAggregateAccumulator<T> extends AggregateAccumulator<T> {
    /** Operand. */
    private Expression operand;

    /** Operand type. */
    protected transient DataType operandType;

    /** Distinct operands. */
    private transient Set<Object> distinctSet;

    public SingleAggregateAccumulator() {
        // No-op.
    }

    public SingleAggregateAccumulator(boolean distinct, Expression operand) {
        super(distinct);

        this.operand = operand;
    }

    @Override
    public void setup(AggregateExec parent) {
        super.setup(parent);

        if (distinct)
            distinctSet = new HashSet<>();
    }

    @Override
    public T eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        // Null operands are not processed.
        if (operandValue == null)
            return null;

        // Check for uniqueness.
        if (distinct && !isDistinct(operandValue))
            return null;

        // Resolve types.
        if (operandType == null) {
            operandType = operand.getType();

            resType = resolveReturnType(operandType);
        }

        // Collect.
        collect(operandValue);

        // Accumulators do not return anything.
        return null;
    }

    /**
     * Collect new value.
     *
     * @param value New value.
     */
    protected abstract void collect(Object value);

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
