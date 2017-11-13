package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * A {@code RaftException} which is thrown when a query request cannot be executed for some reason.
 * <p>
 * One reason to reject a query is when there's no commit in the Raft state yet.
 */
public class CannotRunLocalQueryException extends RaftException {
    public CannotRunLocalQueryException(RaftEndpoint leader) {
        super(leader);
    }
}
