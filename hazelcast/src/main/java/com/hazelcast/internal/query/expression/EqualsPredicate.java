package com.hazelcast.internal.query.expression;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class EqualsPredicate implements Predicate {

    private Expression left;
    private Expression right;

    public EqualsPredicate() {
        // No-op.
    }

    public EqualsPredicate(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Boolean eval(QueryContext ctx, Row row) {
        // TODO: Is this semantics correct from NULL handling perspective?
        // TODO: Incorrect type conversions and type inference!
        Object leftObj = left.eval(ctx, row);
        Object rightObj = right.eval(ctx, row);

        return leftObj == null ? rightObj == null : leftObj.equals(rightObj);
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
