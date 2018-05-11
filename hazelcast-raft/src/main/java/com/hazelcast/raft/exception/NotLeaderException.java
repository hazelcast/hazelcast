package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.RaftMember;

/**
 * A {@code RaftException} which is thrown when a leader-only request is received by a non-leader member.
 */
public class NotLeaderException extends RaftException {
    public NotLeaderException(RaftGroupId groupId, RaftMember local, RaftMember leader) {
        super(local + " is not LEADER of " + groupId + ". Known leader is: "
                + (leader != null ? leader : "N/A") , leader);
    }
}
