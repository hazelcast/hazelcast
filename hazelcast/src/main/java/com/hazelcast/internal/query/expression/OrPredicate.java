package com.hazelcast.internal.query.expression;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class OrPredicate implements Predicate {

    private Predicate left;
    private Predicate right;

    public OrPredicate() {
        // No-op.
    }

    public OrPredicate(Predicate left, Predicate right) {
        this.left = left;
        this.right = right;
    }

    @Override public Boolean eval(QueryContext ctx, Row row) {
        return left.eval(ctx, row) || right.eval(ctx, row);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();
    }
}
