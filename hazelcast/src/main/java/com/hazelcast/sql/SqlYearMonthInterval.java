package com.hazelcast.sql;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Interval data type.
 */
public class SqlYearMonthInterval implements DataSerializable, Comparable<SqlYearMonthInterval> {
    /** Months. */
    private int months;

    public SqlYearMonthInterval() {
        // No-op.
    }

    public SqlYearMonthInterval(int months) {
        this.months = months;
    }

    public int getMonths() {
        return months;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(months);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        months = in.readInt();
    }

    @Override
    public int hashCode() {
        return months;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SqlYearMonthInterval) {
            SqlYearMonthInterval other = ((SqlYearMonthInterval)obj);

            return months == other.months;
        }

        return false;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(SqlYearMonthInterval other) {
        return Integer.compare(months, other.months);
    }

    @Override
    public String toString() {
        return "SqlYearMonthInterval{months=" + months + "}";
    }
}
