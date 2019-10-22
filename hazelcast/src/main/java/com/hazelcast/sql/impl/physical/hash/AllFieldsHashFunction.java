package com.hazelcast.sql.impl.physical.hash;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;

/**
 * Hash function which uses all row columns to calculate the hash.
 */
public class AllFieldsHashFunction implements HashFunction, DataSerializable {
    /** Singleton instance. */
    public static AllFieldsHashFunction INSTANCE = new AllFieldsHashFunction();

    public AllFieldsHashFunction() {
        // No-op.
    }

    @Override
    public int getHash(Row row) {
        int res = 0;

        for (int idx = 0; idx < row.getColumnCount(); idx++) {
            Object val = row.getColumn(idx);
            int hash = val != null ? val.hashCode() : 0;

            res = 31 * res + hash;
        }

        return res;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // No-op.
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        // No-op.
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AllFieldsHashFunction;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{}";
    }
}
