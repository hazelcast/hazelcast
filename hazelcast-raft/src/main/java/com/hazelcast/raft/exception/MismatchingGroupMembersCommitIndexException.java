package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftMember;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;

/**
 * A {@code RaftException} which is thrown when a membership change is requested
 * but expected members commitIndex doesn't match the actual members commitIndex in the Raft state.
 */
public class MismatchingGroupMembersCommitIndexException extends RaftException {

    private transient long commitIndex;

    private transient Collection<RaftMember> members;

    public MismatchingGroupMembersCommitIndexException(long commitIndex, Collection<RaftMember> members) {
        super(null);
        this.commitIndex = commitIndex;
        this.members = members;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public Collection<RaftMember> getMembers() {
        return members;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeLong(commitIndex);
        out.writeInt(members.size());
        for (RaftMember endpoint : members) {
            out.writeObject(endpoint);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        commitIndex = in.readLong();
        int count = in.readInt();
        members = new HashSet<RaftMember>(count);
        for (int i = 0; i < count; i++) {
            members.add((RaftMember) in.readObject());
        }
    }
}
