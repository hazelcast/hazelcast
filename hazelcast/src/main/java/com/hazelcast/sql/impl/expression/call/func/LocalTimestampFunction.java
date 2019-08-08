package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.call.CallExpression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;
import java.time.OffsetDateTime;

/**
 * Function to get local timestamp.
 */
public class LocalTimestampFunction implements CallExpression<OffsetDateTime> {
    @Override
    public OffsetDateTime eval(QueryContext ctx, Row row) {
        return OffsetDateTime.now();
    }

    @Override
    public DataType getType() {
        return DataType.TIMESTAMP_WITH_TIMEZONE_OFFSET_DATE_TIME;
    }

    @Override
    public int operator() {
        return CallOperator.CURRENT_TIMESTAMP;
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
