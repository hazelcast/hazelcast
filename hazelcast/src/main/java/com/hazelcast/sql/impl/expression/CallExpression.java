package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Function call.
 */
public class CallExpression<T> implements Expression<T> {
    /** Operator. */
    private int operator;

    /** Operands. */
    private List<Expression> operands;

    public CallExpression() {
        // No-op.
    }

    public CallExpression(int operator, List<Expression> operands) {
        this.operator = operator;
        this.operands = operands;
    }

    @Override
    public T eval(QueryContext ctx, Row row) {
        T res;

        switch (operator) {
            case CallOperator.PLUS: {
                Object op1 = operands.get(0).eval(ctx, row);
                Object op2 = operands.get(1).eval(ctx, row);

                if (op1 instanceof Number && op2 instanceof Number) {
                    long res0 = ((Number)op1).longValue() + ((Number)op2).longValue();

                    // TODO: Awful!
                    //noinspection unchecked
                    res = (T)(Object)res0;
                }
                else
                    // TODO: Proper exception.
                    throw new UnsupportedOperationException("Invalid data types for the plus operand.");

                break;
            }

            default:
                // TODO: Proper exception.
                throw new UnsupportedOperationException("Operation is not supported: " + operator);
        }

        return res;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(operator);

        out.writeInt(operands.size());

        for (Expression operand : operands)
            out.writeObject(operand);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operator = in.readInt();

        int operandCnt = in.readInt();

        if (operandCnt == 0)
            operands = Collections.emptyList();
        else {
            operands = new ArrayList<>(operandCnt);

            for (int i = 0; i < operandCnt; i++) {
                Expression operand = in.readObject();

                operands.add(operand);
            }
        }
    }
}
