package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.impl.expression.Expression;

/**
 * Function call.
 */
public interface CallExpression<T> extends Expression<T> {
    /**
     * @return Operator.
     */
    int operator();
}
