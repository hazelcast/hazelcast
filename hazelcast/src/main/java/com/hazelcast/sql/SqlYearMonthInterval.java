package com.hazelcast.sql;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Interval data type.
 */
public class SqlYearMonthInterval implements DataSerializable {
    /** Type. */
    private SqlYearMonthIntervalType type;

    /** Seconds. */
    private int val;

    public SqlYearMonthInterval() {
        // No-op.
    }

    public SqlYearMonthInterval(SqlYearMonthIntervalType type, int val) {
        this.type = type;
        this.val = val;
    }

    public SqlYearMonthIntervalType getType() {
        return type;
    }

    public boolean negative() {
        return val < 0;
    }

    public int years() {
        int val0 = Math.abs(val);

        return val0 / 12;
    }

    public int months() {
        int val0 = Math.abs(val);

        return type == SqlYearMonthIntervalType.MONTH ? val0 : val0 % 12;
    }

    public int value() {
        return val;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.order());
        out.writeInt(val);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = SqlYearMonthIntervalType.byOrder(in.readInt());
        val = in.readInt();
    }

    @Override
    public String toString() {
        return "SqlYearMonthInterval{" + (negative() ? "- " : "") + years() + " YEARS " + months()  + " MONTHS}";
    }
}
