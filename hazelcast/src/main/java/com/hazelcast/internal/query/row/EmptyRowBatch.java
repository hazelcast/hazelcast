package com.hazelcast.internal.query.row;

/**
 * Empty row batch.
 */
public class EmptyRowBatch implements RowBatch {
    /** Singleton instance. */
    public static final EmptyRowBatch INSTANCE = new EmptyRowBatch();

    private EmptyRowBatch() {
        // No-op.
    }

    @Override
    public Row getRow(int idx) {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public int getRowCount() {
        return 0;
    }
}
