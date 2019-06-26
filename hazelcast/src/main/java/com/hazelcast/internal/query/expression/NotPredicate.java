package com.hazelcast.internal.query.expression;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.row.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class NotPredicate implements Predicate {

    private Predicate child;

    public NotPredicate() {
        // No-op.
    }

    public NotPredicate(Predicate child) {
        this.child = child;
    }

    @Override public Boolean eval(QueryContext ctx, Row row) {
        return !child.eval(ctx, row);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(child);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        child = in.readObject();
    }
}
