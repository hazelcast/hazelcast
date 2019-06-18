package com.hazelcast.internal.query.worker.data;

import com.hazelcast.internal.query.QueryId;

public class InboxKey {
    private final QueryId queryId;
    private final int edgeId;
    private final int stripe;

    public InboxKey(QueryId queryId, int edgeId, int stripe) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.stripe = stripe;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InboxKey inboxKey = (InboxKey) o;

        if (edgeId != inboxKey.edgeId) return false;
        if (stripe != inboxKey.stripe) return false;
        return queryId.equals(inboxKey.queryId);
    }

    @Override
    public int hashCode() {
        int result = queryId.hashCode();
        result = 31 * result + edgeId;
        result = 31 * result + stripe;
        return result;
    }
}
