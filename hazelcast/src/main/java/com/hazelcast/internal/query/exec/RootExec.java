package com.hazelcast.internal.query.exec;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.QueryResultConsumer;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.internal.query.worker.data.DataWorker;
import com.hazelcast.internal.query.worker.data.RootDataTask;

/**
 * Root executor which consumes results from the upstream stages and pass them to target consumer.
 */
public class RootExec extends AbstractUpstreamAwareExec {
    /** Worker. */
    private DataWorker worker;

    /** Consumer (user iterator, client listener, etc). */
    private QueryResultConsumer consumer;

    /** Current row batch. */
    private RowBatch curBatch;

    /** Size of the batch. */
    private int curBatchSize = -1;

    /** Position within a batch. */
    private int curBatchPos = -1;

    public RootExec(Exec upstream) {
        super(upstream);
    }

    @Override
    protected void setup1(QueryContext ctx, DataWorker worker) {
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

                switch (advanceUpstream()) {
                    case FETCHED_DONE:
                    case FETCHED:
                        RowBatch batch = upstreamCurrentBatch;
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
                        throw new IllegalStateException("Should not reach this.");
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
        // TODO: Double-check that it is not re-scheduled too often (e.g. with printout).
        RootDataTask task = new RootDataTask(this);

        worker.offer(task);
    }

    /**
     * @return Thread responsible for execution of this task.
     */
    public int getThread() {
        return worker.getThread();
    }
}
