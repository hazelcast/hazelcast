package com.hazelcast.internal.query.mailbox;

import com.hazelcast.internal.query.QueryId;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Set;

/**
 * Inbox which puts requests from different stripes into isolated queues.
 */
public class StripedInbox extends Inbox {
    /** Map from member ID to index. */
    private final HashMap<String, Integer> memberToIdxMap = new HashMap<>();

    /** Batches from members. */
    private final ArrayDeque<SendBatch>[] queues;

    @SuppressWarnings("unchecked")
    public StripedInbox(QueryId queryId, int edgeId, int stripe, Set<String> senderMemberIds, int senderStripeCnt) {
        super(queryId, edgeId, stripe, senderMemberIds.size() * senderStripeCnt);

        // Build inverse map from the member to it's index.
        int memberIdx = 0;

        for (String senderMemberId : senderMemberIds) {
            memberToIdxMap.put(senderMemberId, memberIdx);

            memberIdx += senderStripeCnt;
        }

        // Initialize queues.
        int totalSenderStripeCnt = senderMemberIds.size() * senderStripeCnt;

        queues = new ArrayDeque[totalSenderStripeCnt];

        for (int i = 0; i < totalSenderStripeCnt; i++)
            queues[i] = new ArrayDeque<>(1);
    }

    @Override
    public void onBatch0(String sourceMemberId, int sourceStripe, int sourceThread, SendBatch batch) {
        int idx = memberToIdxMap.get(sourceMemberId) + sourceStripe;

        ArrayDeque<SendBatch> queue = queues[idx];

        queue.add(batch);
    }

    public int getStripeCount() {
        return queues.length;
    }

    public SendBatch poll(int stripe) {
        return queues[stripe].poll();
    }

    @Override
    public String toString() {
        return "StripedInbox {queryId=" + queryId +
            ", edgeId=" + getEdgeId() +
            ", stripe=" + getStripe() +
            ", thread=" + getThread() +
        "}";
    }
}
