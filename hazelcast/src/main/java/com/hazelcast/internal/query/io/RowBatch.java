package com.hazelcast.internal.query.io;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RowBatch implements DataSerializable {

    private List<Row> rows;
    private Map<Integer, RowMetadata> metadatas;
    private boolean last;

    public RowBatch() {
        // No-op.
    }

    public RowBatch(List<Row> rows, Map<Integer, RowMetadata> metadatas, boolean last) {
        this.rows = rows;
        this.metadatas = metadatas;
        this.last = last;
    }

    public List<Row> getRows() {
        return rows;
    }

    public boolean isLast() {
        return last;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(rows);
        out.writeObject(metadatas);
        out.writeBoolean(last);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        rows = in.readObject();
        metadatas = in.readObject();
        last = in.readBoolean();
    }
}
