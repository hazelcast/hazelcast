package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.row.EmptyRowBatch;
import com.hazelcast.internal.query.row.RowBatch;
import com.hazelcast.internal.query.worker.data.DataWorker;

/**
 * Executor which has an upstream executor and hence delegate to it at some stages.
 */
public abstract class AbstractUpstreamAwareExec extends AbstractExec {
    /** Upstream operator. */
    private final Exec upstream;

    /** Current batch returned from the upstream. */
    protected RowBatch upstreamCurrentBatch;

    /** Whether upstream is finished. */
    protected boolean upstreamDone;

    /**
     * Constructor.
     *
     * @param upstream Upstream stage.
     */
    protected AbstractUpstreamAwareExec(Exec upstream) {
        this.upstream = upstream;
    }

    @Override
    protected final void setup0(QueryContext ctx, DataWorker worker) {
        upstream.setup(ctx, worker);

        setup1(ctx, worker);
    }

    protected void setup1(QueryContext ctx, DataWorker worker) {
        // No-op.
    }

    /**
     * Advance upstream stage and return the result.
     *
     * @return Advance result.
     */
    protected IterationResult advanceUpstream() {
        if (upstreamDone)
            return IterationResult.FETCHED_DONE;

        IterationResult res = upstream.advance();

        switch (res) {
            case FETCHED_DONE:
                upstreamDone = true;

                // Fall-through.

            case FETCHED:
                upstreamCurrentBatch = upstream.currentBatch();

                break;

            case WAIT:
                upstreamCurrentBatch = EmptyRowBatch.INSTANCE;

                break;

            default:
                throw new IllegalStateException("Should not reach this.");
        }

        return res;
    }
}
