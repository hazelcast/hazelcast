package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.io.SendBatch;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Set;

public class StripedInbox extends Inbox {
    /** Map from member ID to index. */
    private final HashMap<String, Integer> memberToIdxMap = new HashMap<>();

    /** Batches from members. */
    private final ArrayDeque<SendBatch>[] queues;

    // TODO: Factor out to Inbox base class.
    /** Number of remaining inputs. */
    private int remaining;

    @SuppressWarnings("unchecked")
    public StripedInbox(QueryId queryId, String memberId, int edgeId, int stripe,
        Set<String> senderMemberIds, int senderStripeCnt) {
        super(queryId, memberId, edgeId, stripe);

        int memberIdx = 0;

        for (String senderMemberId : senderMemberIds) {
            memberToIdxMap.put(senderMemberId, memberIdx);

            memberIdx += senderStripeCnt;
        }

        int totalSenderStripeCnt = senderMemberIds.size() * senderStripeCnt;

        queues = new ArrayDeque[totalSenderStripeCnt];

        for (int i = 0; i < totalSenderStripeCnt; i++)
            queues[i] = new ArrayDeque<>();

        remaining = totalSenderStripeCnt;
    }

    public int getStripeCount() {
        return queues.length;
    }

    public SendBatch poll(int stripe) {
        return queues[stripe].poll();
    }

    @Override
    public void onBatch(String sourceMemberId, int sourceStripe, int sourceThread, SendBatch batch) {
        System.out.println(">>> [INBOX] BATCH: " + sourceMemberId + ", size=" + batch.getRows().size() + ", last=" + batch.isLast());

        if (!batch.getRows().isEmpty()) {
            int idx = memberToIdxMap.get(sourceMemberId) + sourceStripe;

            ArrayDeque<SendBatch> queue = queues[idx];

            queue.add(batch);
        }

        if (batch.isLast())
            remaining--;
    }

    @Override
    public boolean closed() {
        return remaining == 0;
    }
}
