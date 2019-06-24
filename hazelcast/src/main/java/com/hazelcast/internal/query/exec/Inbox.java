package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.io.SendBatch;

public abstract class Inbox extends Mailbox {
    protected final QueryId queryId;
    protected final String memberId;

    /** Executor which should be notified when data arrives. */
    private Exec exec;

    protected Inbox(QueryId queryId, String memberId, int edgeId, int stripe) {
        super(edgeId, stripe);

        this.queryId = queryId;
        this.memberId = memberId;
    }

    public abstract void onBatch(String sourceMemberId, int sourceStripe, int sourceThread, SendBatch batch);
    public abstract boolean closed();

    public Exec getExec() {
        return exec;
    }

    public void setExec(Exec exec) {
        this.exec = exec;
    }
}
