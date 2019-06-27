package com.hazelcast.internal.query.expression;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ConstantExpression<T> implements Expression<T> {

    private T val;

    public ConstantExpression() {
        // No-op.
    }

    public ConstantExpression(T val) {
        this.val = val;
    }

    @Override
    public T eval(QueryContext ctx, Row row) {
        return val;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // TODO: What if val is not serializable?
        out.writeObject(val);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        val = in.readObject();
    }
}
