package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.io.SendBatch;

import java.util.ArrayDeque;

/**
 * Inbox which merges batches from all the nodes into a single stream.
 */
public class SingleInbox extends Inbox {
    private final ArrayDeque<SendBatch> batches = new ArrayDeque<>();
    private int remaining;

    public SingleInbox(QueryId queryId, String memberId, int edgeId, int stripe, int remaining) {
        super(queryId, memberId, edgeId, stripe);

        this.remaining = remaining;
    }

    // TODO: Exceptions!
    @Override
    public void onBatch(String sourceMemberId, int sourceStripe, int sourceThread, SendBatch batch) {
        // Batch might be empty in case of last marker.
        if (!batch.getRows().isEmpty()) {
            // TODO: Remove
            System.out.println(">>> INBOX  [ADDED]: " + this + ", sourceStripe=" + sourceStripe + ", sourceThread=" + sourceThread + ": " + batch.getRows().size());

            batches.add(batch);
        }

        if (batch.isLast()) {
            remaining--;

            // TODO: Remove
            System.out.println(">>> INBOX  [CLOSE]: " + this + ": " + remaining);
        }
    }

    @Override
    public boolean closed() {
        return remaining == 0;
    }

    public SendBatch poll() {
        return batches.poll();
    }

    @Override
    public String toString() {
        return "Inbox {edgeId=" + getEdgeId() +
            ", member=" + memberId +
            ", stripe=" + getStripe() +
            ", thread=" + getThread() +
            '}';
    }
}
