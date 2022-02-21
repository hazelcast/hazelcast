/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftRole;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.NopRaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raft.impl.task.InitLeadershipTransferTask;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;

import static com.hazelcast.cp.internal.raft.impl.log.RaftLog.newRaftLog;
import static com.hazelcast.cp.internal.raft.impl.log.RaftLog.restoreRaftLog;
import static com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry.isNonInitial;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableSet;

/**
 * {@code RaftState} is the mutable state maintained by Raft state machine
 * on every node in the group.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public final class RaftState {

    /**
     * Endpoint of this node
     */
    private final RaftEndpoint localEndpoint;

    /**
     * Group id
     */
    private final CPGroupId groupId;

    /**
     * Initial members of the group
     * <p>
     * [PERSISTENT]
     */
    private final Collection<RaftEndpoint> initialMembers;

    /**
     * Used for reflecting persistent-state changes to persistent storage.
     */
    private final RaftStateStore store;

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
     * <p>
     * [PERSISTENT]
     */
    private int term;

    /**
     * Latest known leader endpoint (or null if not known).
     */
    private volatile RaftEndpoint leader;

    /**
     * Index of highest log entry known to be committed (initialized to 0, increases monotonically)
     * <p>
     * [NOT-PERSISTENT] because we can re-calculate commitIndex after restoring logs.
     */
    private long commitIndex;

    /**
     * Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
     * <p>
     * {@code lastApplied <= commitIndex} condition holds true always.
     * <p>
     * [NOT-PERSISTENT] because we can apply restored logs and re-calculate lastApplied.
     */
    private long lastApplied;

    /**
     * Endpoint that received vote in the current term, or null if none
     * <p>
     * [PERSISTENT]
     */
    private RaftEndpoint votedFor;

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
     * becomes null when pre-voting ends by one of {@link #toCandidate(boolean)}, {@link #toLeader()} or {@link #toFollower(int)}.
     */
    private CandidateState preCandidateState;

    /**
     * Candidate state maintained during leader election,
     * initialized when this node becomes candidate via {@link #toCandidate(boolean)} )}
     * and becomes null when voting ends by one of {@link #toLeader()} or {@link #toFollower(int)}.
     */
    private CandidateState candidateState;

    /**
     * State maintained by the current leader during leadership transfer.
     * Initialized when the leadership transfer process is started via
     * {@link InitLeadershipTransferTask} and cleared when the local Raft node
     * switches to a new term or the leadership transfer process times out.
     */
    private LeadershipTransferState leadershipTransferState;

    private RaftState(CPGroupId groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints, int logCapacity,
                      RaftStateStore store) {
        this.groupId = groupId;
        this.localEndpoint = localEndpoint;
        this.initialMembers = unmodifiableSet(new LinkedHashSet<RaftEndpoint>(endpoints));
        RaftGroupMembers groupMembers = new RaftGroupMembers(0, endpoints, localEndpoint);
        this.committedGroupMembers = groupMembers;
        this.lastGroupMembers = groupMembers;
        this.store = store;
        this.log = newRaftLog(logCapacity, store);
    }

    private RaftState(CPGroupId groupId, RestoredRaftState restoredState, int logCapacity, RaftStateStore store) {
        checkNotNull(groupId);
        checkNotNull(restoredState);
        checkNotNull(store);
        this.groupId = groupId;
        this.localEndpoint = restoredState.localEndpoint();
        this.initialMembers = unmodifiableSet(new LinkedHashSet<RaftEndpoint>(restoredState.initialMembers()));
        this.committedGroupMembers = new RaftGroupMembers(0, this.initialMembers, this.localEndpoint);
        this.lastGroupMembers = this.committedGroupMembers;
        this.term = restoredState.term();
        this.votedFor = restoredState.votedFor();

        SnapshotEntry snapshot = restoredState.snapshot();
        if (isNonInitial(snapshot)) {
            RaftGroupMembers groupMembers = new RaftGroupMembers(snapshot.groupMembersLogIndex(), snapshot.groupMembers(),
                    this.localEndpoint);
            this.committedGroupMembers = groupMembers;
            this.lastGroupMembers = groupMembers;
            this.commitIndex = snapshot.index();
            this.lastApplied = snapshot.index();
        }

        this.log = restoreRaftLog(logCapacity, snapshot, restoredState.entries(), store);
        this.store = store;
    }

    public static RaftState newRaftState(CPGroupId groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints,
                                         int logCapacity) {
        return newRaftState(groupId, localEndpoint, endpoints, logCapacity, NopRaftStateStore.INSTANCE);
    }

    public static RaftState newRaftState(CPGroupId groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints,
                                         int logCapacity, RaftStateStore stateStore) {
        return new RaftState(groupId, localEndpoint, endpoints, logCapacity, stateStore);
    }

    public static RaftState restoreRaftState(CPGroupId groupId, RestoredRaftState restoredState, int logCapacity) {
        return restoreRaftState(groupId, restoredState, logCapacity, NopRaftStateStore.INSTANCE);
    }

    public static RaftState restoreRaftState(CPGroupId groupId, RestoredRaftState restoredState, int logCapacity,
                                             RaftStateStore stateStore) {
        return new RaftState(groupId, restoredState, logCapacity, stateStore);
    }

    public String name() {
        return groupId.getName();
    }

    public CPGroupId groupId() {
        return groupId;
    }

    public RaftEndpoint localEndpoint() {
        return localEndpoint;
    }

    public Collection<RaftEndpoint> initialMembers() {
        return initialMembers;
    }

    /**
     * Returns all members in the last applied group members
     */
    public Collection<RaftEndpoint> members() {
        return lastGroupMembers.members();
    }

    /**
     * Returns remote members in the last applied group members
     */
    public Collection<RaftEndpoint> remoteMembers() {
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
     * Returns the state store that persists changes on this Raft state
     */
    public RaftStateStore stateStore() {
        return store;
    }

    /**
     * Returns the known leader
     */
    public RaftEndpoint leader() {
        return leader;
    }

    /**
     * Returns the endpoint this note voted for
     * @see #votedFor
     */
    public RaftEndpoint votedFor() {
        return votedFor;
    }

    /**
     * Initializes the Raft state by initializing the state store
     * and persisting the initial member list
     *
     * @see RaftStateStore#open()
     * @see RaftStateStore#persistInitialMembers(RaftEndpoint, Collection)
     *
     * @throws IOException if an IO error occurs inside the state store
     */
    public void init() throws IOException {
        store.open();
        store.persistInitialMembers(localEndpoint, initialMembers);
    }

    /**
     * Updates the known leader
     */
    public void leader(RaftEndpoint endpoint) {
        leader = endpoint;
        if (endpoint != null) {
            // Since we have a new leader, preCandidateState becomes obsolete.
            preCandidateState = null;
        }
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
    public void persistVote(int term, RaftEndpoint endpoint) {
        assert this.term == term;
        assert this.votedFor == null;
        this.votedFor = endpoint;
        persistTerm();
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
        completeLeadershipTransfer(null);
        setTerm(term);
        persistTerm();
    }

    /**
     * Switches this node to candidate role. Clears pre-candidate and leader states.
     * Initializes candidate state for current majority and grants vote for local endpoint as a candidate.
     *
     * @return vote request to sent to other members during leader election
     */
    public VoteRequest toCandidate(boolean disruptive) {
        role = RaftRole.CANDIDATE;
        preCandidateState = null;
        leaderState = null;
        candidateState = new CandidateState(majority());
        candidateState.grantVote(localEndpoint);
        setTerm(term + 1);
        persistVote(term, localEndpoint);
        // no need to call persistTerm() since it is called in persistVote()

        return new VoteRequest(localEndpoint, term, log.lastLogOrSnapshotTerm(), log.lastLogOrSnapshotIndex(), disruptive);
    }

    private void setTerm(int newTerm) {
        assert newTerm >= term : "New term: " + newTerm + ", current term: " + term;
        if (newTerm > term) {
            term = newTerm;
            votedFor = null;
        }
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
    public boolean isKnownMember(RaftEndpoint endpoint) {
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
     * Removes pre-candidate state
     */
    public void removePreCandidateState() {
        preCandidateState = null;
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
    public void updateGroupMembers(long logIndex, Collection<RaftEndpoint> members) {
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
            for (RaftEndpoint endpoint : members) {
                if (!committedGroupMembers.isKnownMember(endpoint)) {
                    leaderState.add(endpoint, log.lastLogOrSnapshotIndex());
                }
            }

            for (RaftEndpoint endpoint : committedGroupMembers.remoteMembers()) {
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
    public void restoreGroupMembers(long logIndex, Collection<RaftEndpoint> members) {
        assert lastGroupMembers.index() <= logIndex
                : "Cannot restore group members to: " + members + " at log index: " + logIndex + " because last group members: "
                + lastGroupMembers + " has a bigger log index.";

        // there is no leader state to clean up

        RaftGroupMembers groupMembers = new RaftGroupMembers(logIndex, members, localEndpoint);
        this.committedGroupMembers = groupMembers;
        this.lastGroupMembers = groupMembers;
    }

    private void persistTerm() {
        try {
            store.persistTerm(term, votedFor);
        } catch (IOException e) {
            throw new HazelcastException(e);
        }
    }

    /**
     * Initializes the leadership transfer state, and returns {@code true}
     * if the leadership transfer is triggered for the first time
     * and returns {@code false} if there is an ongoing leadership transfer
     * process.
     *
     * @return true if the leadership transfer is triggered for the first time,
     *         false if there is an ongoing leadership transfer
     */
    public boolean initLeadershipTransfer(RaftEndpoint targetEndpoint, InternalCompletableFuture resultFuture) {
        if (leadershipTransferState == null) {
            leadershipTransferState = new LeadershipTransferState(term, targetEndpoint, resultFuture);
            return true;
        }

        leadershipTransferState.notify(targetEndpoint, resultFuture);
        return false;
    }

    /**
     * Completes the current leadership transfer state with the given result
     * and clears the state
     */
    public void completeLeadershipTransfer(Object result) {
        if (leadershipTransferState == null) {
            return;
        }

        leadershipTransferState.complete(result);
        leadershipTransferState = null;
    }

    /**
     * Returns the leadership transfer state
     */
    public LeadershipTransferState leadershipTransferState() {
        return leadershipTransferState;
    }
}
