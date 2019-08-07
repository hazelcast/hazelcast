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
import com.hazelcast.sql.impl.type.TypeUtils;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.io.IOException;

/**
 * Function of single operand which returns a double.
 */
public class DoubleFunction extends UniCallExpression<Double> {
    /** Operator. */
    private int operator;

    /** Operand accessor. */
    private transient Converter accessor;

    public DoubleFunction() {
        // No-op.
    }

    public DoubleFunction(Expression operand, int operator) {
        super(operand);

        this.operator = operator;
    }

    @Override
    public Double eval(QueryContext ctx, Row row) {
        Object res = operand.eval(ctx, row);

        if (res == null)
            return null;
        else if (accessor == null)
            accessor = TypeUtils.numericAccessor(operand);

        double res0 = accessor.asDouble(res);

        switch (operator) {
            case CallOperator.COS:
                return Math.cos(res0);

            case CallOperator.SIN:
                return Math.sin(res0);

            case CallOperator.TAN:
                return Math.tan(res0);

            case CallOperator.COT:
                return 1.0d / Math.tan(res0);

            case CallOperator.ACOS:
                return Math.acos(res0);

            case CallOperator.ASIN:
                return Math.asin(res0);

            case CallOperator.ATAN:
                return Math.atan(res0);

            case CallOperator.SQRT:
                return Math.sqrt(res0);

            case CallOperator.EXP:
                return Math.exp(res0);

            case CallOperator.LN:
                return Math.log(res0);

            case CallOperator.LOG10:
                return Math.log10(res0);

            case CallOperator.DEGREES:
                return Math.toDegrees(res0);

            case CallOperator.RADIANS:
                return Math.toRadians(res0);
        }

        throw new HazelcastSqlException(-1, "Unsupported double operator: " + operator);
    }

    @Override
    public DataType getType() {
        return DataType.DOUBLE;
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