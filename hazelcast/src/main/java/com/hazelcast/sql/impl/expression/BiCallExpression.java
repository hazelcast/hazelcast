package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Expression with two operands.
 */
public abstract class BiCallExpression<T> implements Expression {
    /** First operand. */
    private Expression operand1;

    /** Second operand. */
    private Expression operand2;

    protected BiCallExpression() {
        // No-op.
    }

    protected BiCallExpression(Expression operand1, Expression operand2) {
        this.operand1 = operand1;
        this.operand2 = operand2;
    }

    /**
     * @return Operator.
     */
    public abstract int operator();

    public Expression getOperand1() {
        return operand1;
    }

    public Expression getOperand2() {
        return operand2;
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
