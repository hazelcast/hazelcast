package com.hazelcast.sql;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Interval data type.
 */
public class SqlDaySecondInterval implements DataSerializable {
    /** Type. */
    private SqlDaySecondIntervalType type;

    /** Seconds. */
    private long val;

    /** Fractional component. */
    private int nano;

    public SqlDaySecondInterval() {
        // No-op.
    }

    public SqlDaySecondInterval(SqlDaySecondIntervalType type, long val, int nano) {
        this.type = type;
        this.val = val;
        this.nano = nano;
    }

    public SqlDaySecondIntervalType getType() {
        return type;
    }

    public long value() {
        return val;
    }

    public int nanos() {
        return nano;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.order());
        out.writeLong(val);
        out.writeInt(nano);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = SqlDaySecondIntervalType.byOrder(in.readInt());
        val = in.readLong();
        nano = in.readInt();
    }
}
