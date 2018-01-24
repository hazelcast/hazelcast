package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * A {@code RaftException} which is thrown when an appended but yet not committed entry is truncated by the new leader.
 */
public class LeaderDemotedException extends RaftException {

    public LeaderDemotedException(RaftEndpoint local, RaftEndpoint leader) {
        super(local + " is not LEADER anymore. Known leader is: "
                + (leader != null ? leader : "N/A") , leader);
    }
}
