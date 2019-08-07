package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.TriCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

/**
 * POSITION(seek IN string FROM integer)}.
 */
public class PositionFunction extends TriCallExpression<Integer> {
    /** Accessor of operand 1. */
    private transient Converter accessor1;

    /** Accessor of operand 2. */
    private transient Converter accessor2;

    /** Accessor of operand 3. */
    private transient Converter accessor3;

    public PositionFunction() {
        // No-op.
    }

    public PositionFunction(Expression operand1, Expression operand2, Expression operand3) {
        super(operand1, operand2, operand3);
    }

    @Override
    public Integer eval(QueryContext ctx, Row row) {
        String seek;
        String source;
        int pos;

        // Get seek operand.
        Object op1 = operand1.eval(ctx, row);

        if (op1 == null)
            return null;

        if (accessor1 == null)
            accessor1 = operand1.getType().getBaseType().getAccessor();

        seek = accessor1.asVarchar(op1);

        // Get source operand.
        Object op2 = operand2.eval(ctx, row);

        if (op2 == null)
            return null;

        if (accessor2 == null)
            accessor2 = operand2.getType().getBaseType().getAccessor();

        source = accessor2.asVarchar(op2);

        // Get "FROM"
        if (operand3 == null)
            pos = 0;
        else {
            Object op3 = operand3.eval(ctx, row);

            if (op3 == null)
                pos = 0;
            else {
                if (accessor3 == null) {
                    DataType type = operand3.getType();

                    if (!type.isNumericInteger())
                        throw new HazelcastSqlException(-1, "Unsupported data type: " + type);

                    accessor3 = type.getBaseType().getAccessor();
                }

                pos = accessor3.asInt(op3);
            }
        }

        // Process.
        if (pos == 0)
            return source.indexOf(seek) + 1;
        else {
            int pos0 = pos - 1;

            if (pos0 < 0 || pos0 > source.length())
                return 0;

            return source.indexOf(seek, pos0) + 1;
        }
    }

    @Override
    public DataType getType() {
        return DataType.VARCHAR;
    }

    @Override
    public int operator() {
        return CallOperator.POSITION;
    }
}
