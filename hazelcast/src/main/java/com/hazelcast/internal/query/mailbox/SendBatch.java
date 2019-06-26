package com.hazelcast.internal.query.mailbox;

import com.hazelcast.internal.query.row.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// TODO: Implement metadata!.
public class SendBatch implements DataSerializable {
    /** Rows being transferred. */
    private List<Row> rows;

    /** Laft batch marker. */
    private boolean last;

    public SendBatch() {
        // No-op.
    }

    public SendBatch(List<Row> rows, boolean last) {
        this.rows = rows;
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
        out.writeInt(rows.size());

        for (Row row : rows)
            // TODO: "writeObject" appears to be too heavy. Find a way to avoid that (e.g. factories).
            out.writeObject(row);

        out.writeBoolean(last);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int rowCnt = in.readInt();

        if (rowCnt == 0)
            rows = Collections.emptyList();
        else {
            rows = new ArrayList<>(rowCnt);

            for (int i = 0; i < rowCnt; i++)
                rows.add(in.readObject());
        }

        last = in.readBoolean();
    }
}
