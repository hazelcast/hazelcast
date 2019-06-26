package com.hazelcast.internal.query.row;

/**
 * Batch of rows.
 */
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
