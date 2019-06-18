package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.io.RowBatch;

import java.util.ArrayDeque;

public class Inbox extends Mailbox {
    private final QueryId queryId;
    private final String memberId;
    private final ArrayDeque<RowBatch> batches = new ArrayDeque<>();
    private int remaining;

    /** Executor which should be notified when data arrives. */
    private Exec exec;

    public Inbox(QueryId queryId, String memberId, int edgeId, int stripe, int remaining) {
        super(edgeId, stripe);

        this.queryId = queryId;
        this.memberId = memberId;
        this.remaining = remaining;
    }

    // TODO: Exceptions!

    public void onBatch(RowBatch batch) {
        // Batch might be empty in case of last marker.
        if (!batch.getRows().isEmpty()) {
            System.out.println(">>> INBOX  [ADDED]: " + this + ": " + batch.getRows());

            batches.add(batch);
        }

        if (batch.isLast()) {
            remaining--;

            System.out.println(">>> INBOX  [CLOSE]: " + this + ": " + remaining);
        }
    }

    @Override
    public String toString() {
        return "Inbox {edgeId=" + getEdgeId() +
            ", member=" + memberId +
            ", stripe=" + getStripe() +
            ", thread=" + getThread() +
        '}';
    }

    public RowBatch poll() {
        return batches.poll();
    }

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
