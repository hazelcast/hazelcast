package com.hazelcast.internal.query.io;

/**
 * Empty row batch.
 */
public class EmptyRowBatch implements RowBatch {

    public static final EmptyRowBatch INSTANCE = new EmptyRowBatch();

    private EmptyRowBatch() {
        // No-op.
    }

    @Override
    public SerializableRow getRow(int idx) {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public int getRowCount() {
        return 0;
    }
}
