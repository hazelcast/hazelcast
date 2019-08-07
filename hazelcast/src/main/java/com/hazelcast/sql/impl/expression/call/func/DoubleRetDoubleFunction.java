package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Function of single operand which returns a double.
 */
public class DoubleRetDoubleFunction extends UniCallExpression<Double> {
    /** Operator. */
    private int operator;

    /** Operand type. */
    private transient DataType operandType;

    public DoubleRetDoubleFunction() {
        // No-op.
    }

    public DoubleRetDoubleFunction(Expression operand, int operator) {
        super(operand);

        this.operator = operator;
    }

    @Override
    public Double eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null)
            return null;
        else if (operandType == null) {
            DataType type = operand.getType();

            if (!type.isCanConvertToNumeric())
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand is not numeric: " + type);

            operandType = type;
        }

        double operandValueDouble = operandType.getConverter().asDouble(operandValue);

        switch (operator) {
            case CallOperator.COS:
                return Math.cos(operandValueDouble);

            case CallOperator.SIN:
                return Math.sin(operandValueDouble);

            case CallOperator.TAN:
                return Math.tan(operandValueDouble);

            case CallOperator.COT:
                return 1.0d / Math.tan(operandValueDouble);

            case CallOperator.ACOS:
                return Math.acos(operandValueDouble);

            case CallOperator.ASIN:
                return Math.asin(operandValueDouble);

            case CallOperator.ATAN:
                return Math.atan(operandValueDouble);

            case CallOperator.SQRT:
                return Math.sqrt(operandValueDouble);

            case CallOperator.EXP:
                return Math.exp(operandValueDouble);

            case CallOperator.LN:
                return Math.log(operandValueDouble);

            case CallOperator.LOG10:
                return Math.log10(operandValueDouble);

            case CallOperator.DEGREES:
                return Math.toDegrees(operandValueDouble);

            case CallOperator.RADIANS:
                return Math.toRadians(operandValueDouble);
        }

        throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
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