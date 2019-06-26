package com.hazelcast.internal.query.physical;

import com.hazelcast.cluster.Member;

public class SendDestination {

    private final Member member;
    private final int stripe;
    private int thread;

    public SendDestination(Member member, int stripe) {
        this.member = member;
        this.stripe = stripe;
    }

    public Member getMember() {
        return member;
    }

    public int getStripe() {
        return stripe;
    }

    public int getThread() {
        return thread;
    }

    /**
     * Callback invoked when the thread for the given strip is resolved.
     *
     */
    private void onThreadResolved(int thread) {
        this.thread = thread;
    }
}
