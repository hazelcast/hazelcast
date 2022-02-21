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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.Collection;

/**
 * {@code RaftNode} maintains the state of a member for a specific Raft group
 * and exposes methods to handle external client requests (such as append
 * requests, queries and membership changes) and internal Raft RPCs (voting,
 * append request, snapshot installing etc).
 */
public interface RaftNode {

    /**
     * Returns the groupId which this node belongs to.
     */
    CPGroupId getGroupId();

    /**
     * Returns the Raft endpoint for this node.
     */
    RaftEndpoint getLocalMember();

    /**
     * Returns the known leader endpoint. Leader endpoint might be already
     * changed when this method returns.
     */
    RaftEndpoint getLeader();

    /**
     * Returns the current status of this node.
     */
    RaftNodeStatus getStatus();

    /**
     * Returns the initial member list of the Raft group this node belongs to.
     */
    Collection<RaftEndpoint> getInitialMembers();

    /**
     * Returns the last committed member list of the raft group this node
     * belongs to. Please note that the returned member list can be different
     * from the currently effective member list, if there is an ongoing
     * membership change in the group.
     */
    Collection<RaftEndpoint> getCommittedMembers();

    /**
     * Returns the currently effective member list of the raft group this node
     * belongs to. Please note that the returned member list can be different
     * from the committed member list, if there is an ongoing
     * membership change in the group.
     */
    Collection<RaftEndpoint> getAppliedMembers();

    /**
     * Returns true if this node is {@link RaftNodeStatus#TERMINATED} or
     * {@link RaftNodeStatus#STEPPED_DOWN}, false otherwise.
     * <p>
     * This method is essentially same as;
     * <pre>
     * <code>
     *     return status == TERMINATED || status == STEPPED_DOWN
     * </code>
     * </pre>
     */
    boolean isTerminatedOrSteppedDown();

    /**
     * Sets node's status to {@link RaftNodeStatus#TERMINATED} unconditionally
     * if it's not terminated or stepped down yet.
     */
    InternalCompletableFuture forceSetTerminatedStatus();

    /**
     * Handles {@link PreVoteRequest} sent by another follower.
     */
    void handlePreVoteRequest(PreVoteRequest request);

    /**
     * Handles {@link PreVoteResponse} for a previously sent request by
     * this node.
     */
    void handlePreVoteResponse(PreVoteResponse response);

    /**
     * Handles {@link VoteRequest} sent by a candidate.
     */
    void handleVoteRequest(VoteRequest request);

    /**
     * Handles {@link VoteResponse} for a previously sent vote request by
     * this node.
     */
    void handleVoteResponse(VoteResponse response);

    /**
     * Handles {@link AppendRequest} sent by leader.
     */
    void handleAppendRequest(AppendRequest request);

    /**
     * Handles {@link AppendSuccessResponse} for a previously sent
     * append request by this node.
     */
    void handleAppendResponse(AppendSuccessResponse response);

    /**
     * Handles {@link AppendFailureResponse} for a previously sent
     * append request by this node.
     */
    void handleAppendResponse(AppendFailureResponse response);

    /**
     * Handles {@link InstallSnapshot} sent by leader.
     */
    void handleInstallSnapshot(InstallSnapshot request);

    void handleTriggerLeaderElection(TriggerLeaderElection request);

    /**
     * Replicates the given operation to the Raft group.
     * Only the leader can process replicate requests.
     * <p>
     * Otherwise, if this node is not leader, or the leader is demoted before
     * committing the operation, the returned future is notified with a related
     * exception.
     *
     * @param operation operation to replicate
     * @return future to get notified about result of the replication
     */
    InternalCompletableFuture replicate(Object operation);

    /**
     * Replicates the membership change to the Raft group.
     * Only the leader can process membership change requests.
     * <p>
     * If this node is not leader, or the leader is demoted before committing
     * the operation, or membership change is not committed for any reason,
     * then the returned future is notified with a related exception.
     *
     * @param member member to add or remove
     * @param mode   type of membership change
     * @return future to get notified about result of the membership change
     */
    InternalCompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeMode mode);

    /**
     * Replicates the membership change to the Raft group, if expected members
     * commit index is equal to the actual one stored in Raft state. Otherwise,
     * fails with {@link MismatchingGroupMembersCommitIndexException}.
     * <p>
     * For more info see {@link #replicate(Object)}.
     *
     * @param member                  member to add or remove
     * @param mode                    type of membership change
     * @param groupMembersCommitIndex expected members commit index
     * @return future to get notified about result of the membership change
     */
    InternalCompletableFuture replicateMembershipChange(RaftEndpoint member,
                                                        MembershipChangeMode mode,
                                                        long groupMembersCommitIndex);

    /**
     * Executes the given operation on Raft group depending
     * on the {@link QueryPolicy}
     *
     * @param operation operation to query
     * @param queryPolicy query policy to decide where to execute operation
     * @return future to get notified about result of the query
     */
    InternalCompletableFuture query(Object operation, QueryPolicy queryPolicy);

    /**
     * Transfers group leadership to the given endpoint, if the local Raft node
     * is the leader with ACTIVE status and the endpoint is a group member.
     * <p>
     * Leadership transfer is considered to be completed when the local Raft
     * node moves to a term that is bigger than its current term, and there is
     * no strict guarantee that the given endpoint will be the new leader.
     * However, it is very likely that the given endpoint will become
     * the new leader.
     * <p>
     * The local Raft node will not replicate any new entry during a leadership
     * transfer and new calls to the {@link #replicate(Object)} method will
     * fail with {@link CannotReplicateException}.
     *
     * @throws IllegalArgumentException if the endpoint is not a group member
     * @throws IllegalStateException    if the local Raft node is not leader,
     *                                  or the Raft node status is not ACTIVE,
     *                                  or the leader transfer has timed out.
     *
     * @return future to get notified about result of the leadership transfer
     */
    InternalCompletableFuture transferLeadership(RaftEndpoint endpoint);
}
