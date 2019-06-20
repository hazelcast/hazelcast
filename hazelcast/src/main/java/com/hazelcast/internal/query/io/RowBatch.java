package com.hazelcast.internal.query.io;

public interface RowBatch {
    /**
     * Get row at the given index.
     *
     * @param idx Index.
     * @return Row.
     */
    Row getRow(int idx);

    /**
     * @return Number of rows.
     */
    int getRowCount();
}
