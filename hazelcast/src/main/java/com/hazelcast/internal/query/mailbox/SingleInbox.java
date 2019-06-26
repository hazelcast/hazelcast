package com.hazelcast.internal.query.mailbox;

import com.hazelcast.internal.query.QueryId;

import java.util.ArrayDeque;

/**
 * Inbox which merges batches from all the nodes into a single stream.
 */
public class SingleInbox extends Inbox {
    /** Queue of batches from all remote stripes. */
    private final ArrayDeque<SendBatch> batches = new ArrayDeque<>();

    public SingleInbox(QueryId queryId, int edgeId, int stripe, int remaining) {
        super(queryId, edgeId, stripe, remaining);
    }

    @Override
    public void onBatch0(String sourceMemberId, int sourceStripe, int sourceThread, SendBatch batch) {
        batches.add(batch);
    }

    public SendBatch poll() {
        return batches.poll();
    }

    @Override
    public String toString() {
        return "SingleInbox {queryId=" + queryId +
            ", edgeId=" + getEdgeId() +
            ", stripe=" + getStripe() +
            ", thread=" + getThread() +
        "}";
    }
}
