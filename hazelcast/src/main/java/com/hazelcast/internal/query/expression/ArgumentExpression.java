package com.hazelcast.internal.query.expression;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ArgumentExpression<T> implements Expression<T> {

    private int idx;

    public ArgumentExpression() {
        // No-op.
    }

    public ArgumentExpression(int idx) {
        this.idx = idx;
    }

    @Override
    public T eval(QueryContext ctx, Row row) {
        // TODO: Type-check
        return (T)ctx.getArgument(idx);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(idx);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        idx = in.readInt();
    }
}
