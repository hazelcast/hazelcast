package com.hazelcast.internal.query.mailbox;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.exec.Exec;

public abstract class Inbox extends Mailbox {
    /** Executor which should be notified when data arrives. */
    private Exec exec;

    /** Remaining remote sources. */
    private int remaining;

    protected Inbox(QueryId queryId, int edgeId, int stripe, int remaining) {
        super(queryId, edgeId, stripe);

        this.remaining = remaining;
    }

    /**
     * Handle batch arrival. Always invoked from {@link com.hazelcast.internal.query.worker.data.DataWorker}.
     */
    public void onBatch(String sourceMemberId, int sourceStripe, int sourceThread, SendBatch batch) {
        // Batch might be empty in case of last marker.
        if (!batch.getRows().isEmpty())
            onBatch0(sourceMemberId, sourceStripe, sourceThread, batch);

        if (batch.isLast())
            remaining--;
    }

    protected abstract void onBatch0(String sourceMemberId, int sourceStripe, int sourceThread, SendBatch batch);

    /**
     * @return {@code True} if no more incoming batches are expected.
     */
    public boolean closed() {
        return remaining == 0;
    }

    public Exec getExec() {
        return exec;
    }

    public void setExec(Exec exec) {
        this.exec = exec;
    }
}
