package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;

/**
 * Expression with three operands.
 */
public abstract class TriCallExpression<T> implements CallExpression<T> {
    /** First operand. */
    protected Expression operand1;

    /** Second operand. */
    protected Expression operand2;

    /** Third operand. */
    protected Expression operand3;

    protected TriCallExpression() {
        // No-op.
    }

    protected TriCallExpression(Expression operand1, Expression operand2, Expression operand3) {
        this.operand1 = operand1;
        this.operand2 = operand2;
        this.operand3 = operand3;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(operand1);
        out.writeObject(operand2);
        out.writeObject(operand3);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operand1 = in.readObject();
        operand2 = in.readObject();
        operand3 = in.readObject();
    }
}
