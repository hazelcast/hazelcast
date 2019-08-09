package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.BiCallExpression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Predicates: IS NULL / IS NOT NULL / IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
 */
public class AndOrPredicate extends BiCallExpression<Boolean> {
    /** Operator. */
    private boolean or;

    /** Whether the first operand is checked. */
    private transient boolean operand1Checked;

    /** Whether the second operand is checked. */
    private transient boolean operand2Checked;

    public AndOrPredicate() {
        // No-op.
    }

    public AndOrPredicate(Expression operand1, Expression operand2, boolean or) {
        super(operand1, operand2);

        this.or = or;
    }

    @Override
    public Boolean eval(QueryContext ctx, Row row) {
        Object operand1Value = operand1.eval(ctx, row);

        if (operand1Value == null)
            return null;
        else if (!operand1Checked) {
            if (operand1.getType() != DataType.BIT)
                throw new HazelcastSqlException(-1, "Operand 1 is not BIT.");

            operand1Checked = true;
        }

        Object operand2Value = operand2.eval(ctx, row);

        if (operand2Value == null)
            return null;
        else if (!operand2Checked) {
            if (operand2.getType() != DataType.BIT)
                throw new HazelcastSqlException(-1, "Operand 2 is not BIT.");

            operand2Checked = true;
        }

        boolean first = (boolean)operand1Value;
        boolean second = (boolean)operand2Value;

        return or ? first || second : first && second;
    }

    @Override
    public DataType getType() {
        return DataType.BIT;
    }

    @Override
    public int operator() {
        return or ? CallOperator.OR : CallOperator.AND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(or);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        or = in.readBoolean();
    }
}