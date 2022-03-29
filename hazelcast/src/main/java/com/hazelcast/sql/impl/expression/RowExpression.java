package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.List;

public class RowExpression extends VariExpressionWithType<RowValue> {

    public RowExpression() { }

    private RowExpression(Expression<?>[] operands) {
        super(operands, QueryDataType.ROW);
    }

    public static RowExpression create(Expression<?>[] operands) {
        return new RowExpression(operands);
    }

    @Override
    public RowValue eval(final Row row, final ExpressionEvalContext context) {
        final List<Object> values = new ArrayList<>();
        for (final Expression<?> operand : operands) {
            values.add(operand.eval(row, context));
        }

        return new RowValue(values);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.ROW;
    }
}
