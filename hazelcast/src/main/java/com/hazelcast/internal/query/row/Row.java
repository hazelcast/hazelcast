package com.hazelcast.internal.query.row;

/**
 * Single row.
 */
public interface Row extends RowBatch {
    /**
     * Get column by index.
     *
     * @param idx Index.
     * @return Column value.
     */
    Object getColumn(int idx);

    /**
     * @return Number of columns in a row.
     */
    int getColumnCount();

    @Override
    default Row getRow(int idx) {
        return this;
    }

    @Override
    default int getRowCount() {
        return 1;
    }
}
