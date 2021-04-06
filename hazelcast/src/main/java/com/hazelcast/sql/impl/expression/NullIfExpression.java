package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

public class NullIfExpression<T> implements Expression<T>, IdentifiedDataSerializable {
    private Expression<T> left;
    private Expression<T> right;

    public NullIfExpression() {
    }

    public static NullIfExpression<?> create(Expression<?> left, Expression<?> right) {
        return new NullIfExpression(left, right);
    }

    private NullIfExpression(Expression<T> left, Expression<T> right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }

    @Override
    public int getFactoryId() {
        return 0;
    }

    @Override
    public int getClassId() {
        return 0;
    }

    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        T leftResult = left.eval(row, context);
        T rightResult = right.eval(row, context);
        if (leftResult.equals(rightResult)) {
            return null;
        }
        return leftResult;
    }

    @Override
    public QueryDataType getType() {
        return left.getType();
    }
}
