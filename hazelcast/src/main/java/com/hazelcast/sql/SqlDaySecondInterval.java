package com.hazelcast.sql;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Interval data type.
 */
public class SqlDaySecondInterval implements DataSerializable, Comparable<SqlDaySecondInterval> {
    /** Seconds. */
    private long seconds;

    /** Nanos. */
    private int nanos;

    public SqlDaySecondInterval() {
        // No-op.
    }

    public SqlDaySecondInterval(long seconds, int nanos) {
        this.seconds = seconds;
        this.nanos = nanos;
    }

    public long getSeconds() {
        return seconds;
    }

    public int getNanos() {
        return nanos;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(seconds);
        out.writeInt(nanos);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        seconds = in.readLong();
        nanos = in.readInt();
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(seconds) + nanos;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SqlDaySecondInterval) {
            SqlDaySecondInterval other = ((SqlDaySecondInterval)obj);

            return seconds == other.seconds && nanos == other.nanos;
        }

        return false;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(SqlDaySecondInterval other) {
        int res = Long.compare(seconds, other.seconds);

        if (res == 0)
            res = Integer.compare(nanos, other.nanos);

        return res;
    }

    @Override
    public String toString() {
        return "SqlDaySecondInterval{seconds=" + seconds + ", nanos=" + nanos + "}";
    }
}
