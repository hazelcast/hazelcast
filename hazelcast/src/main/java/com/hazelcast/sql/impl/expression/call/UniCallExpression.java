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
public abstract class UniCallExpression<T> implements Expression<T> {
    /** Operand. */
    protected Expression operand;

    protected UniCallExpression() {
        // No-op.
    }

    protected UniCallExpression(Expression operand) {
        this.operand = operand;
    }

    /**
     * @return Operator.
     */
    public abstract int operator();

    public Expression getOperand() {
        return operand;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(operand);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operand = in.readObject();
    }
}
