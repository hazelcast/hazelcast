package com.hazelcast.internal.query.expression;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ColumnExpression<T> implements Expression<T> {

    private int idx;

    public ColumnExpression() {
        // No-op.
    }

    public ColumnExpression(int idx) {
        this.idx = idx;
    }

    @Override public T eval(QueryContext ctx, Row row) {
        // TODO: Type-check?
        return (T)row.get(idx);
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
