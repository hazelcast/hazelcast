/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.cluster.Endpoint;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftRole;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;

import java.util.Collection;
import java.util.LinkedHashSet;

import static java.util.Collections.unmodifiableSet;

/**
 * {@code RaftState} is the mutable state maintained by Raft state machine
 * on every node in the group.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class RaftState {

    /**
     * Endpoint of this node
     */
    private final Endpoint localEndpoint;

    /**
     * Group id
     */
    private final CPGroupId groupId;

    /**
     * Initial members of the group
     */
    private final Collection<Endpoint> initialMembers;

    /**
     * Latest committed group members.
     */
    private RaftGroupMembers committedGroupMembers;

    /**
     * Latest applied group members (initially equal to {@link #committedGroupMembers})
     */
    private RaftGroupMembers lastGroupMembers;

    /**
     * Role of this node in the group
     */
    private RaftRole role = RaftRole.FOLLOWER;

    /**
     * Latest term this node has seen (initialized to 0 on first boot, increases monotonically)
     */
    private int term;

    /**
     * Latest known leader endpoint (or null if not known).
     */
    private volatile Endpoint leader;

    /**
     * Index of highest log entry known to be committed (initialized to 0, increases monotonically)
     */
    private long commitIndex;

    /**
     * Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
     * <p>
     * {@code lastApplied <= commitIndex} condition holds true always.
     */
    private long lastApplied;

    /**
     * Endpoint that received vote in {@link #lastVoteTerm} (or null if none)
     */
    private Endpoint votedFor;

    /**
     * Term that granted vote for {@link #votedFor}
     */
    private int lastVoteTerm;

    /**
     * Raft log entries; each entry contains command for state machine,
     * and term when entry was received by leader (first index is 1)
     */
    private final RaftLog log;

    /**
     * State maintained by the leader, null if this node is not the leader
     */
    private LeaderState leaderState;

    /**
     * Candidate state maintained during pre-voting,
     * becomes null when pre-voting ends by one of {@link #toCandidate()}, {@link #toLeader()} or {@link #toFollower(int)}.
     */
    private CandidateState preCandidateState;

    /**
     * Candidate state maintained during leader election,
     * initialized when this node becomes candidate via {@link #toCandidate()}
     * and becomes null when voting ends by one of {@link #toLeader()} or {@link #toFollower(int)}.
     */
    private CandidateState candidateState;

    public RaftState(CPGroupId groupId, Endpoint localEndpoint, Collection<Endpoint> endpoints, int logCapacity) {
        this.groupId = groupId;
        this.localEndpoint = localEndpoint;
        this.initialMembers = unmodifiableSet(new LinkedHashSet<>(endpoints));
        RaftGroupMembers groupMembers = new RaftGroupMembers(0, endpoints, localEndpoint);
        this.committedGroupMembers = groupMembers;
        this.lastGroupMembers = groupMembers;
        this.log = new RaftLog(logCapacity);
    }

    public String name() {
        return groupId.name();
    }

    public CPGroupId groupId() {
        return groupId;
    }

    public Collection<Endpoint> initialMembers() {
        return initialMembers;
    }

    /**
     * Returns all members in the last applied group members
     */
    public Collection<Endpoint> members() {
        return lastGroupMembers.members();
    }

    /**
     * Returns remote members in the last applied group members
     */
    public Collection<Endpoint> remoteMembers() {
        return lastGroupMembers.remoteMembers();
    }

    /**
     * Returns number of members in the last applied group members
     */
    public int memberCount() {
        return lastGroupMembers.memberCount();
    }

    /**
     * Returns majority number of the last applied group members
     */
    public int majority() {
        return lastGroupMembers.majority();
    }

    /**
     * Returns log index of the last applied group members
     */
    public long membersLogIndex() {
        return lastGroupMembers.index();
    }

    /**
     * Returns committed group members
     */
    public RaftGroupMembers committedGroupMembers() {
        return committedGroupMembers;
    }

    /**
     * Returns the last applied group members
     */
    public RaftGroupMembers lastGroupMembers() {
        return lastGroupMembers;
    }

    /**
     * Returns role of this node in the group.
     */
    public RaftRole role() {
        return role;
    }

    /**
     * Returns the latest term this node has seen
     */
    public int term() {
        return term;
    }

    /**
     * Increment term by 1
     */
    int incrementTerm() {
        return ++term;
    }

    /**
     * Returns the known leader
     */
    public Endpoint leader() {
        return leader;
    }

    /**
     * Returns the term when this note voted for endpoint {@link #votedFor}
     * @see #lastVoteTerm
     */
    public int lastVoteTerm() {
        return lastVoteTerm;
    }

    /**
     * Returns the endpoint this note voted for
     * @see #votedFor
     */
    public Endpoint votedFor() {
        return votedFor;
    }

    /**
     * Updates the known leader
     */
    public void leader(Endpoint endpoint) {
        leader = endpoint;
    }

    /**
     * Returns the index of highest log entry known to be committed
     * @see #commitIndex
     */
    public long commitIndex() {
        return commitIndex;
    }

    /**
     * Updates commit index
     * @see #commitIndex
     */
    public void commitIndex(long index) {
        assert index >= commitIndex : "new commit index: " + index + " is smaller than current commit index: " + commitIndex;
        commitIndex = index;
    }

    /**
     * Returns the index of highest log entry applied to state machine
     * @see #lastApplied
     */
    public long lastApplied() {
        return lastApplied;
    }

    /**
     * Updates the last applied index
     * @see #lastApplied
     */
    public void lastApplied(long index) {
        assert index >= lastApplied : "new last applied: " + index + " is smaller than current last applied: " + lastApplied;
        lastApplied = index;
    }

    /**
     * Returns the Raft log
     */
    public RaftLog log() {
        return log;
    }

    /**
     * Returns the leader state
     */
    public LeaderState leaderState() {
        return leaderState;
    }

    /**
     * Returns the candidate state
     */
    public CandidateState candidateState() {
        return candidateState;
    }

    /**
     * Persist a vote for the endpoint in current term during leader election.
     */
    public void persistVote(int term, Endpoint endpoint) {
        this.lastVoteTerm = term;
        this.votedFor = endpoint;
    }

    /**
     * Switches this node to follower role. Clears leader and (pre)candidate states, updates the term.
     *
     * @param term current term
     */
    public void toFollower(int term) {
        role = RaftRole.FOLLOWER;
        leader = null;
        preCandidateState = null;
        leaderState = null;
        candidateState = null;
        this.term = term;
    }

    /**
     * Switches this node to candidate role. Clears pre-candidate and leader states.
     * Initializes candidate state for current majority and grants vote for local endpoint as a candidate.
     *
     * @return vote request to sent to other members during leader election
     */
    public VoteRequest toCandidate() {
        role = RaftRole.CANDIDATE;
        preCandidateState = null;
        leaderState = null;
        candidateState = new CandidateState(majority());
        candidateState.grantVote(localEndpoint);
        persistVote(incrementTerm(), localEndpoint);

        return new VoteRequest(localEndpoint, term, log.lastLogOrSnapshotTerm(), log.lastLogOrSnapshotIndex());
    }

    /**
     * Switches this node to leader role. Sets local endpoint as current leader.
     * Clears (pre)candidate states. Initializes leader state for current members.
     */
    public void toLeader() {
        role = RaftRole.LEADER;
        leader(localEndpoint);
        preCandidateState = null;
        candidateState = null;
        leaderState = new LeaderState(lastGroupMembers.remoteMembers(), log.lastLogOrSnapshotIndex());
    }

    /**
     * Returns true if the endpoint is a member of the last applied group, false otherwise.
     */
    public boolean isKnownMember(Endpoint endpoint) {
        return lastGroupMembers.isKnownMember(endpoint);
    }

    /**
     * Initializes pre-candidate state for pre-voting and grant a vote for local endpoint
     */
    public void initPreCandidateState() {
        preCandidateState = new CandidateState(majority());
        preCandidateState.grantVote(localEndpoint);
    }

    /**
     * Returns pre-candidate state
     */
    public CandidateState preCandidateState() {
        return preCandidateState;
    }

    /**
     * Initializes the last applied group members with the members and logIndex.
     * This method expects there's no uncommitted membership changes, committed members are the same as
     * the last applied members.
     *
     * Leader state is updated for the members which don't exist in committed members and committed members
     * those don't exist in latest applied members are removed.
     *
     * @param logIndex log index of membership change
     * @param members latest applied members
     */
    public void updateGroupMembers(long logIndex, Collection<Endpoint> members) {
        assert committedGroupMembers == lastGroupMembers
                : "Cannot update group members to: " + members + " at log index: " + logIndex + " because last group members: "
                + lastGroupMembers + " is different than committed group members: " + committedGroupMembers;
        assert lastGroupMembers.index() < logIndex
                : "Cannot update group members to: " + members + " at log index: " + logIndex + " because last group members: "
                + lastGroupMembers + " has a bigger log index.";

        RaftGroupMembers newGroupMembers = new RaftGroupMembers(logIndex, members, localEndpoint);
        committedGroupMembers = lastGroupMembers;
        lastGroupMembers = newGroupMembers;

        if (leaderState != null) {
            for (Endpoint endpoint : members) {
                if (!committedGroupMembers.isKnownMember(endpoint)) {
                    leaderState.add(endpoint, log.lastLogOrSnapshotIndex());
                }
            }

            for (Endpoint endpoint : committedGroupMembers.remoteMembers()) {
                if (!members.contains(endpoint)) {
                    leaderState.remove(endpoint);
                }
            }
        }
    }

    /**
     * Marks the last applied group members as committed. At this point {@link #committedGroupMembers}
     * and {@link #lastGroupMembers} are the same.
     */
    public void commitGroupMembers() {
        assert committedGroupMembers != lastGroupMembers
                : "Cannot commit last group members: " + lastGroupMembers + " because it is same with committed group members";

        committedGroupMembers = lastGroupMembers;
    }

    /**
     * Resets {@link #lastGroupMembers} back to {@link #committedGroupMembers}. Essentially this means,
     * applied but uncommitted membership changes are reverted.
     */
    public void resetGroupMembers() {
        assert this.committedGroupMembers != this.lastGroupMembers;

        this.lastGroupMembers = this.committedGroupMembers;
        // there is no leader state to clean up
    }

    /**
     * Restores group members from the snapshot. Both {@link #committedGroupMembers}
     * and {@link #lastGroupMembers} are overwritten and they become the same.
     */
    public void restoreGroupMembers(long logIndex, Collection<Endpoint> members) {
        assert lastGroupMembers.index() <= logIndex
                : "Cannot restore group members to: " + members + " at log index: " + logIndex + " because last group members: "
                + lastGroupMembers + " has a bigger log index.";

        // there is no leader state to clean up

        RaftGroupMembers groupMembers = new RaftGroupMembers(logIndex, members, localEndpoint);
        this.committedGroupMembers = groupMembers;
        this.lastGroupMembers = groupMembers;
    }
}
