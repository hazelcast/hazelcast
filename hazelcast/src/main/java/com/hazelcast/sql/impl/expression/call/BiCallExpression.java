package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.TypeUtils;

import java.io.IOException;

/**
 * Expression with two operands.
 */
public abstract class BiCallExpression<T> implements CallExpression<T> {
    /** First operand. */
    protected Expression operand1;

    /** Second operand. */
    protected Expression operand2;

    /** Result type. */
    // TODO: Refactor?
    protected transient DataType resType;

    protected BiCallExpression() {
        // No-op.
    }

    protected BiCallExpression(Expression operand1, Expression operand2) {
        this.operand1 = operand1;
        this.operand2 = operand2;
    }

    @Override
    public DataType getType() {
        return TypeUtils.notNull(resType);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(operand1);
        out.writeObject(operand2);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operand1 = in.readObject();
        operand2 = in.readObject();
    }
}
