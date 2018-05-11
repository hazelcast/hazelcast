package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftMember;

/**
 * A {@code RaftException} which is thrown when an appended but yet not committed entry is truncated
 * after a snapshot is installed.
 */
public class StaleAppendRequestException extends RaftException {

    public StaleAppendRequestException(RaftMember leader) {
        super(leader);
    }
}
