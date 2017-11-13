package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * A {@code RaftException} which is thrown when an appended but yet not committed entry is truncated
 * after a snapshot is installed.
 */
public class StaleAppendRequestException extends RaftException {

    public StaleAppendRequestException(RaftEndpoint leader) {
        super(leader);
    }
}
