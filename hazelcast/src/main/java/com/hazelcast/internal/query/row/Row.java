package com.hazelcast.internal.query.row;

import com.hazelcast.sql.SqlRow;

/**
 * Single row.
 */
public interface Row extends RowBatch, SqlRow {
    @Override
    default Row getRow(int idx) {
        return this;
    }

    @Override
    default int getRowCount() {
        return 1;
    }
}
