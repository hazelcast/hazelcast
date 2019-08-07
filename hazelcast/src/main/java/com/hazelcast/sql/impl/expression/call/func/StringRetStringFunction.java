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
import java.util.Locale;

/**
 * A function which accepts a string, and return another string.
 */
public class StringRetStringFunction extends UniCallExpression<String> {
    /** Operator. */
    private int operator;

    /** Operand type. */
    private transient DataType operandType;

    public StringRetStringFunction() {
        // No-op.
    }

    public StringRetStringFunction(Expression operand, int operator) {
        super(operand);

        this.operator = operator;
    }

    @Override
    public String eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null)
            return null;

        if (operandType == null)
            operandType = operand.getType();

        String operandValueString = operandType.getConverter().asVarchar(operandValue);

        switch (operator) {
            case CallOperator.UPPER:
                return operandValueString.toUpperCase(Locale.ROOT);

            case CallOperator.LOWER:
                return operandValueString.toLowerCase(Locale.ROOT);

            case CallOperator.INITCAP:
                return doCapitalize(operandValueString);

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    /**
     * Do capitalization.
     *
     * @param operandValue Input string.
     * @return Result.
     */
    private static String doCapitalize(String operandValue) {
        if (operandValue.length() == 0)
            return operandValue;

        int strLen = operandValue.length();

        StringBuilder res = new StringBuilder(strLen);

        boolean capitalizeNext = true;

        for (int i = 0; i < strLen; i++) {
            char c = operandValue.charAt(i);

            if (Character.isWhitespace(c)) {
                res.append(c);

                capitalizeNext = true;
            }
            else if (capitalizeNext) {
                res.append(Character.toTitleCase(c));

                capitalizeNext = false;
            }
            else
                res.append(c);
        }

        return res.toString();
    }

    @Override
    public DataType getType() {
        return DataType.VARCHAR;
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
