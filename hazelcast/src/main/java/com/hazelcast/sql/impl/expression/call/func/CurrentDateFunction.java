package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallExpression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;
import java.time.LocalDate;

/**
 * Function to get current date.
 */
public class CurrentDateFunction implements CallExpression<LocalDate> {
    @Override
    public LocalDate eval(QueryContext ctx, Row row) {
        return LocalDate.now();
    }

    @Override
    public DataType getType() {
        return DataType.DATE;
    }

    @Override
    public int operator() {
        return CallOperator.CURRENT_DATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // No-op.
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        // No-op.
    }
}
