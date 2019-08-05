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
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

import java.io.IOException;
import java.util.Locale;

/**
 * A function which accepts a string, and return another string.
 */
public class StringStringFunction extends UniCallExpression<String> {
    /** Operator. */
    private int operator;

    /** Accessor. */
    private transient BaseDataTypeAccessor accessor;

    public StringStringFunction() {
        // No-op.
    }

    public StringStringFunction(Expression operand, int operator) {
        super(operand);

        this.operator = operator;
    }

    @Override
    public String eval(QueryContext ctx, Row row) {
        Object op = operand.eval(ctx, row);

        if (op == null)
            return null;

        if (accessor == null)
            accessor = operand.getType().getBaseType().getAccessor();

        String res = accessor.getString(op);

        switch (operator) {
            case CallOperator.UPPER:
                return res.toUpperCase(Locale.ROOT);

            case CallOperator.LOWER:
                return res.toLowerCase(Locale.ROOT);

            case CallOperator.INITCAP:
                return capitalize(res);

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    private static String capitalize(String str) {
        if (str.length() == 0)
            return str;

        int strLen = str.length();

        StringBuilder res = new StringBuilder(strLen);

        boolean capitalizeNext = true;

        for (int i = 0; i < strLen; i++) {
            char c = str.charAt(i);

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
