package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftMember;

/**
 * A {@code RaftException} which is thrown when an appended but yet not committed entry is truncated by the new leader.
 */
public class LeaderDemotedException extends RaftException {

    public LeaderDemotedException(RaftMember local, RaftMember leader) {
        super(local + " is not LEADER anymore. Known leader is: "
                + (leader != null ? leader : "N/A") , leader);
    }
}
