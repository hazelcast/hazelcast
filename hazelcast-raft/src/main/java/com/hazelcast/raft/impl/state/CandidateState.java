package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CandidateState {

    private final int majority;
    private final Set<RaftEndpoint> voters = new HashSet<RaftEndpoint>();

    public CandidateState(int majority) {
        this.majority = majority;
    }

    public boolean grantVote(RaftEndpoint address) {
        return voters.add(address);
    }

    public int majority() {
        return majority;
    }

    public int voteCount() {
        return voters.size();
    }

    public boolean isMajorityGranted() {
        return voteCount() >= majority();
    }
}
