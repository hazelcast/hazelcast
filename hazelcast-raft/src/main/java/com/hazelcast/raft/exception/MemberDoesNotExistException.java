package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftMember;

/**
 * A {@code RaftException} which is thrown when a member, which is requested to be removed from a Raft group,
 * is not a member of that group or is already removed from that group.
 */
public class MemberDoesNotExistException extends RaftException {

    public MemberDoesNotExistException(RaftMember member) {
        super("Member does not exist: " + member, null);
    }
}
