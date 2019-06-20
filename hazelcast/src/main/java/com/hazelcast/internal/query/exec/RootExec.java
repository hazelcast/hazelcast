package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.RowBatch;
import com.hazelcast.internal.query.worker.data.DataWorker;
import com.hazelcast.internal.query.worker.data.RootDataTask;

public class RootExec extends AbstractExec {
    /** Actual executor. */
    private final Exec upstream;

    /** Worker. */
    private DataWorker worker;

    /** User consumer. */
    private RootConsumer consumer;

    /** Whether executor is finished. */
    private boolean upstreamDone;

    /** Current row batch. */
    private RowBatch curBatch;

    /** Size of the batch. */
    private int curBatchSize = -1;

    /** Position within a batch. */
    private int curBatchPos = -1;

    public RootExec(Exec upstream) {
        this.upstream = upstream;
    }

    @Override
    protected void setup0(QueryContext ctx, DataWorker worker) {
        upstream.setup(ctx, worker);

        this.worker = worker;

        consumer = ctx.getRootConsumer();

        consumer.setup(this);
    }

    @Override
    public IterationResult advance() {
        while (true) {
            if (curBatch == null) {
                if (upstreamDone) {
                    consumer.done();

                    return IterationResult.FETCHED_DONE;
                }

                switch (upstream.advance()) {
                    case FETCHED_DONE:
                        upstreamDone = true;

                        // Fall-through.

                    case FETCHED:
                        RowBatch batch = upstream.currentBatch();
                        int batchSize = batch.getRowCount();

                        if (batchSize == 0)
                            continue;

                        curBatch = batch;
                        curBatchSize = batchSize;
                        curBatchPos = 0;

                        break;

                    case WAIT:
                        return IterationResult.WAIT;

                    default:
                        // TODO: Propagate error to the user.
                        throw new UnsupportedOperationException("Unsupported.");
                }
            }

            assert curBatch != null;

            if (!consumeBatch())
                return IterationResult.WAIT;
        }
    }

    @Override
    public RowBatch currentBatch() {
        throw new UnsupportedOperationException("Should not be called.");
    }

    /**
     * Try consuming current batch.
     */
    private boolean consumeBatch() {
        int consumed = consumer.consume(curBatch, curBatchPos);

        curBatchPos += consumed;

        if (curBatchPos == curBatchSize) {
            curBatch = null;
            curBatchPos = -1;
            curBatchSize = -1;

            return true;
        }
        else
            return false;
    }

    /**
     * Reschedule execution of this root node to fetch more data to the user.
     */
    public void reschedule() {
        worker.offer(new RootDataTask(worker.getThread(), this));
    }
}
