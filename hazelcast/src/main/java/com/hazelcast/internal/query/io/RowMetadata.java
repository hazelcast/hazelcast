package com.hazelcast.internal.query.io;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RowMetadata implements DataSerializable {

    private List<DataType> rows;

    public RowMetadata(List<DataType> rows) {
        this.rows = rows;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // TODO: No need to write this
        out.writeInt(rows.size());

        for (DataType row : rows)
            out.writeInt(row.ordinal()); // TODO: Compatibility when new types are added!
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        rows = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            rows.add(DataType.fromOrdinal(in.readInt()));
    }
}
