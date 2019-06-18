package com.hazelcast.internal.query;

import com.hazelcast.internal.query.io.Row;

/**
 * Final rows consumer.
 */
public interface ResultConsumer {
    /**
     * Consume a single row.
     *
     * @param row Row.
     * @return {@code True} if more rows could be consumed.
     */
    boolean onRow(Row row);

    /**
     * Notify consumer that some rows were added.
     */
    void onRowsAdded();

    /**
     * Signal consumer that no more rows are expected.
     *
     * @param err Optional error.
     */
    void onDone(RuntimeException err);
}
