package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.log.RaftLog;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftState {

    private final RaftEndpoint localEndpoint;
    private final String name;
    private final Collection<RaftEndpoint> members;
    private final Collection<RaftEndpoint> remoteMembers;

    private RaftRole role = RaftRole.FOLLOWER;
    private int term;

    // defined as volatile to be able to read without synchronization
    private volatile RaftEndpoint leader;

    // index of highest committed log entry
    private int commitIndex;

    // index of highest log entry that's applied to state
    // lastApplied <= commitIndex
    private int lastApplied;

    private RaftEndpoint votedFor;
    private int lastVoteTerm;

    private RaftLog log = new RaftLog();

    private LeaderState leaderState;
    private CandidateState candidateState;

    public RaftState(String name, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints) {
        this.name = name;
        this.localEndpoint = localEndpoint;
        this.members = unmodifiableSet(new HashSet<RaftEndpoint>(endpoints));
        Set<RaftEndpoint> remoteMembers = new HashSet<RaftEndpoint>(endpoints);
        boolean removed = remoteMembers.remove(localEndpoint);
        assert removed : "Members set must contain local member! Members: " + endpoints + ", Local member: " + localEndpoint;
        this.remoteMembers = unmodifiableSet(remoteMembers);
    }

    public String name() {
        return name;
    }

    public Collection<RaftEndpoint> members() {
        return members;
    }

    public Collection<RaftEndpoint> remoteMembers() {
        return remoteMembers;
    }

    public int memberCount() {
        return members.size();
    }

    public int majority() {
        return members.size() / 2 + 1;
    }

    public RaftRole role() {
        return role;
    }

    public int term() {
        return term;
    }

    public int incrementTerm() {
        return ++term;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int lastVoteTerm() {
        return lastVoteTerm;
    }

    public RaftEndpoint votedFor() {
        return votedFor;
    }

    public void leader(RaftEndpoint endpoint) {
        leader = endpoint;
    }

    public int commitIndex() {
        return commitIndex;
    }

    public void commitIndex(int index) {
        assert index >= commitIndex : "new commit index: " + index + " is smaller than current commit index: " + commitIndex;
        commitIndex = index;
    }

    public int lastApplied() {
        return lastApplied;
    }

    public void lastApplied(int index) {
        assert index >= lastApplied : "new last applied: " + index + " is smaller than current last applied: " + lastApplied;
        lastApplied = index;
    }

    public RaftLog log() {
        return log;
    }

    public LeaderState leaderState() {
        return leaderState;
    }

    public CandidateState candidateState() {
        return candidateState;
    }

    public void persistVote(int term, RaftEndpoint endpoint) {
        this.lastVoteTerm = term;
        this.votedFor = endpoint;
    }

    public void toFollower(int term) {
        role = RaftRole.FOLLOWER;
        leader = null;
        leaderState = null;
        candidateState = null;
        this.term = term;
    }

    public VoteRequest toCandidate() {
        role = RaftRole.CANDIDATE;
        leaderState = null;
        candidateState = new CandidateState(majority());
        candidateState.grantVote(localEndpoint);
        persistVote(incrementTerm(), localEndpoint);

        return new VoteRequest(localEndpoint, term, log.lastLogTerm(), log.lastLogIndex());
    }

    public void toLeader() {
        role = RaftRole.LEADER;
        leader(localEndpoint);
        candidateState = null;
        leaderState = new LeaderState(remoteMembers, log.lastLogIndex());
    }

    public boolean isKnownEndpoint(RaftEndpoint endpoint) {
        return members.contains(endpoint);
    }
}
