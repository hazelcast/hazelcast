package com.hazelcast.raft.exception;

/**
 * A {@code RaftException} which is thrown when a request is sent to a terminated Raft group.
 */
public class RaftGroupTerminatedException extends RaftException {

    public RaftGroupTerminatedException() {
        super(null);
    }
}
