package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.HashSet;
import java.util.Set;

/**
 * Mutable state maintained by each candidate during pre-voting & voting phases.
 */
public class CandidateState {

    private final int majority;
    private final Set<RaftEndpoint> voters = new HashSet<RaftEndpoint>();

    public CandidateState(int majority) {
        this.majority = majority;
    }

    /**
     * Persists vote for the endpoint during election.
     * This method is idempotent, multiple votes from the same point are counted only once.
     *
     * @return false if endpoint is already voted, true otherwise
     */
    public boolean grantVote(RaftEndpoint address) {
        return voters.add(address);
    }

    /**
     * Returns the number of expected majority of the votes.
     */
    public int majority() {
        return majority;
    }

    /**
     * Returns current granted number of the votes.
     */
    public int voteCount() {
        return voters.size();
    }

    /**
     * Returns true if majority of the votes are granted, false otherwise.
     */
    public boolean isMajorityGranted() {
        return voteCount() >= majority();
    }
}
