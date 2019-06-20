package com.hazelcast.internal.query.io;

import java.util.List;

// TODO: Array based? Reuse for large result sets (i.e. able to reset?)
public class HeapRowBatch implements RowBatch {

    private final List<Row> rows;

    public HeapRowBatch(List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public Row getRow(int idx) {
        return rows.get(idx);
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }
}
