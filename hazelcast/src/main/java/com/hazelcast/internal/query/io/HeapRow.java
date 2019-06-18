package com.hazelcast.internal.query.io;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;

public class HeapRow implements SerializableRow {

    private Object[] values;

    public HeapRow() {
        // No-op.
    }

    public HeapRow(int valCnt) {
        this.values = new Object[valCnt];
    }

    @Override
    public Object get(int idx) {
        return values[idx];
    }

    @Override
    public int columnCount() {
        return values.length;
    }

    public void set(int idx, Object val) {
        values[idx] = val;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // TODO: Size should be written only once on the batch level.

        out.writeObject(values);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        values = in.readObject();
    }

    @Override
    public String toString() {
        return "Rows {" + Arrays.toString(values) + '}';
    }
}
