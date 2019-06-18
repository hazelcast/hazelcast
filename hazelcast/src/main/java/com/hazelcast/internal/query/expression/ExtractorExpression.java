package com.hazelcast.internal.query.expression;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.KeyValueRow;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ExtractorExpression<T> implements Expression<T> {

    private String path;

    public ExtractorExpression() {
        // No-op.
    }

    public ExtractorExpression(String path) {
        this.path = path;
    }

    @Override
    public T eval(QueryContext ctx, Row row) {
        assert row instanceof KeyValueRow;

        KeyValueRow row0 = (KeyValueRow)row;

        return (T)row0.get(path);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(path);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        path = in.readUTF();
    }
}
