package com.hazelcast.sql;

/**
 * Single SQL row.
 */
public interface SqlRow {
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
}
