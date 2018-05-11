package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftMember;

/**
 * A {@code RaftException} which is thrown when a member, which is requested to be added to a Raft group,
 * is already member of that group.
 */
public class MemberAlreadyExistsException extends RaftException {

    public MemberAlreadyExistsException(RaftMember member) {
        super("Member already exists: " + member, null);
    }
}
