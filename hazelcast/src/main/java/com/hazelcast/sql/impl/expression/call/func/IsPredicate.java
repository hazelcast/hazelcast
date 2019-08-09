package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Predicates: IS NULL / IS NOT NULL / IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
 */
public class IsPredicate extends UniCallExpression<Boolean> {
    /** Operator. */
    private int operator;

    /** Whether the operand is checked. */
    private transient boolean operandChecked;

    public IsPredicate() {
        // No-op.
    }

    public IsPredicate(Expression operand, int operator) {
        super(operand);

        this.operator = operator;
    }

    @Override
    public Boolean eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        switch (operator) {
            case CallOperator.IS_NULL:
                return operandValue == null;

            case CallOperator.IS_NOT_NULL:
                return operandValue != null;

            case CallOperator.IS_FALSE:
                return isTrueFalse(operandValue, operand.getType(), true, false);

            case CallOperator.IS_NOT_FALSE:
                return isTrueFalse(operandValue, operand.getType(), true, true);

            case CallOperator.IS_TRUE:
                return isTrueFalse(operandValue, operand.getType(), false, false);

            case CallOperator.IS_NOT_TRUE:
                return isTrueFalse(operandValue, operand.getType(), false, true);
        }

        return operandValue != null;
    }

    @SuppressWarnings("SimplifiableConditionalExpression")
    private boolean isTrueFalse(Object operand, DataType type, boolean isFalse, boolean isNot) {
        Boolean operand0 = (Boolean)operand;

        boolean res;

        if (operand0 == null)
            res = false;
        else {
            if (operandChecked) {
                if (type != DataType.BIT)
                    throw new HazelcastSqlException(-1, "Operand is not BIT.");

                operandChecked = true;
            }

            res = isFalse ? !operand0 : operand0;
        }

        return isNot ? !res : res;
    }

    @Override
    public DataType getType() {
        return DataType.BIT;
    }

    @Override
    public int operator() {
        return operator;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(operator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        operator = in.readInt();
    }
}
