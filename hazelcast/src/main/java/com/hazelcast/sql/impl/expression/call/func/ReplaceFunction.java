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
public class ReplaceFunction extends TriCallExpression<String> {
    /** Accessor of operand 1. */
    private transient Converter accessor1;

    /** Accessor of operand 2. */
    private transient Converter accessor2;

    /** Accessor of operand 3. */
    private transient Converter accessor3;

    public ReplaceFunction() {
        // No-op.
    }

    public ReplaceFunction(Expression operand1, Expression operand2, Expression operand3) {
        super(operand1, operand2, operand3);
    }

    @Override
    public String eval(QueryContext ctx, Row row) {
        String source;
        String search;
        String replacement;

        // Get source operand.
        Object op1 = operand1.eval(ctx, row);

        if (op1 == null)
            return null;

        if (accessor1 == null)
            accessor1 = operand1.getType().getBaseType().getAccessor();

        source = accessor1.asVarchar(op1);

        // Get search operand.
        Object op2 = operand2.eval(ctx, row);

        if (op2 == null)
            return null;

        if (accessor2 == null)
            accessor2 = operand2.getType().getBaseType().getAccessor();

        search = accessor2.asVarchar(op2);

        if (search.isEmpty())
            throw new HazelcastSqlException(-1, "Invalid operand: search cannot be empty.");

        // Get replacement operand.
        Object op3 = operand3.eval(ctx, row);

        if (op3 == null)
            return null;

        if (accessor3 == null)
            accessor3 = operand3.getType().getBaseType().getAccessor();

        replacement = accessor3.asVarchar(op3);

        // Process.
        return source.replace(search, replacement);
    }

    @Override
    public int operator() {
        return CallOperator.REPLACE;
    }

    @Override
    public DataType getType() {
        return DataType.VARCHAR;
    }
}
