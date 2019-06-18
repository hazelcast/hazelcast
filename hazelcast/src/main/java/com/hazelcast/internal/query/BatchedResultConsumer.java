package com.hazelcast.internal.query;

import com.hazelcast.internal.query.io.Row;

import java.util.concurrent.LinkedBlockingQueue;

public class BatchedResultConsumer implements ResultConsumer {
    /** Number of root elements. */
    private final int rootCnt;

    /** Queue with elements. */
    private final LinkedBlockingQueue<Row> queue;

    /** Mutex to handle concurrent operations. */
    private final Object mux = new Object();

    public BatchedResultConsumer(int rootCnt, int batchSize) {
        this.rootCnt = rootCnt;

        queue = new LinkedBlockingQueue<>(batchSize);
    }

    @Override
    public boolean onRow(Row row) {
        return queue.offer(row);
    }

    @Override
    public void onRowsAdded() {

    }

    @Override
    public void onDone(RuntimeException err) {

    }
}
