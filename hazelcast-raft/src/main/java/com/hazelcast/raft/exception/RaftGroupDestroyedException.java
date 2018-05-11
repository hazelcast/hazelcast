package com.hazelcast.raft.exception;

/**
 * A {@code RaftException} which is thrown when a request is sent to a destroyed
 * Raft group.
 */
public class RaftGroupDestroyedException extends RaftException {
    public RaftGroupDestroyedException() {
        super(null);
    }
}
