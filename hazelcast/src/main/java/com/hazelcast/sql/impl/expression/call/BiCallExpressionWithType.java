package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;

public abstract class BiCallExpressionWithType<T> extends BiCallExpression<T> {
    /** Result type. */
    protected transient DataType resType;

    protected BiCallExpressionWithType() {
        // No-op.
    }

    protected BiCallExpressionWithType(Expression operand1, Expression operand2) {
        this.operand1 = operand1;
        this.operand2 = operand2;
    }

    @Override
    public DataType getType() {
        return DataType.notNullOrLate(resType);
    }
}
