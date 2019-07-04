package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface RaftStateStore extends Closeable {

    void writeTermAndVote(int currentTerm, RaftEndpoint votedFor, int voteTerm) throws IOException;

    void writeInitialMembers(RaftEndpoint localMember, Collection<RaftEndpoint> initialMembers) throws IOException;

    void writeGroupMembers(RaftEndpoint localMember, RaftGroupMembers committedGroupMembers, RaftGroupMembers lastGroupMembers)
            throws IOException;

    RaftLogStore getRaftLogStore();
}
