package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.impl.type.DataType;

/**
 * Expression with result type field.
 */
public abstract class UniCallExpressionWithType<T> extends UniCallExpression<T> {
    /** Result type. */
    protected transient DataType resType;

    protected UniCallExpressionWithType() {
        // No-op.
    }

    protected UniCallExpressionWithType(Expression operand) {
        this.operand = operand;
    }

    @Override
    public DataType getType() {
        return DataType.notNullOrLate(resType);
    }
}
