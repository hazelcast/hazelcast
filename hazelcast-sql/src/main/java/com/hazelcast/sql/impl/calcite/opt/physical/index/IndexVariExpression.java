package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("rawtypes")
public class IndexVariExpression extends VariExpression<Set<Comparable>> {
    public IndexVariExpression() {
        // No-op.
    }

    public IndexVariExpression(Expression<?>... operands) {
        super(operands);
    }

    @Override
    public Set<Comparable> eval(Row row, ExpressionEvalContext context) {
        Set<Comparable> res = new HashSet<>();

        for (Expression<?> operand : operands) {
            Object value = operand.eval(row, context);

            // TODO: Be careful with cast!
            res.add((Comparable) value);
        }

        return res;
    }

    @Override
    public QueryDataType getType() {
        // TODO: Is it valid? Or may be we need some for of inference here?
        return operands[0].getType();
    }

    public Expression<?>[] getOperands() {
        return operands;
    }

    // TODO: Identified data serializable
}
