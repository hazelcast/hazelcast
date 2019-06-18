package com.hazelcast.internal.query.exec;

public abstract class Mailbox {
    /** Edge ID. */
    private final int edgeId;
    
    /** Stripe (known in advance). */
    private final int stripe;
    
    /** Thread (known after prepare). */
    private int thread;

    public Mailbox(int edgeId, int stripe) {
        this.edgeId = edgeId;
        this.stripe = stripe;
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
