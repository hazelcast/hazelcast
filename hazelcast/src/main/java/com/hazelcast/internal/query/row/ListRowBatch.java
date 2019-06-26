package com.hazelcast.internal.query.row;

import java.util.List;

/**
 * Batch where rows are organized in a list.
 */
public class ListRowBatch implements RowBatch {
    /** Rows. */
    private final List<Row> rows;

    public ListRowBatch(List<Row> rows) {
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
