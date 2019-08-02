package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.TypeUtils;

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
        return TypeUtils.notNull(resType);
    }
}
