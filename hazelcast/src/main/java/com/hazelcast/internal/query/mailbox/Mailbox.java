package com.hazelcast.internal.query.mailbox;

import com.hazelcast.internal.query.QueryId;

/**
 * Base class for inboxes and outboxes.
 */
public abstract class Mailbox {
    /** Query ID. */
    protected final QueryId queryId;

    /** Edge ID. */
    protected final int edgeId;
    
    /** Stripe (known in advance). */
    protected final int stripe;
    
    /** Thread (known after prepare). */
    private int thread;

    public Mailbox(QueryId queryId, int edgeId, int stripe) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.stripe = stripe;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public int getStripe() {
        return stripe;
    }

    public int getThread() {
        return thread;
    }

    public void setThread(int thread) {
        this.thread = thread;
    }
}
