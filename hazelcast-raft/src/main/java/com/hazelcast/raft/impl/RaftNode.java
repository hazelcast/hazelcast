package com.hazelcast.raft.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.operation.RaftOperation;

/**
 * {@code RaftNode} maintains the state of a member for a specific Raft group
 * and exposes methods to handle external client requests (such as append requests, queries and membership changes)
 * and internal Raft RPCs (voting, append request, snapshot installing etc).
 */
public interface RaftNode {

    /**
     * Returns the groupId which this node belongs to.
     */
    RaftGroupId getGroupId();

    /**
     * Returns the Raft endpoint for this node.
     */
    RaftEndpoint getLocalEndpoint();

    /**
     * Returns the known leader endpoint. Leader endpoint might be already changed when this method returns.
     */
    RaftEndpoint getLeader();

    /**
     * Returns the current status of this node.
     */
    RaftNodeStatus getStatus();

    /**
     * Returns true if this node is {@link RaftNodeStatus#TERMINATED} or {@link RaftNodeStatus#STEPPED_DOWN},
     * false otherwise.
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
    void forceSetTerminatedStatus();

    /**
     * Handles {@link PreVoteRequest} sent by another follower.
     */
    void handlePreVoteRequest(PreVoteRequest request);

    /**
     * Handles {@link PreVoteResponse} for a previously sent request by this node.
     */
    void handlePreVoteResponse(PreVoteResponse response);

    /**
     * Handles {@link VoteRequest} sent by a candidate.
     */
    void handleVoteRequest(VoteRequest request);

    /**
     * Handles {@link VoteResponse} for a previously sent vote request by this node.
     */
    void handleVoteResponse(VoteResponse response);

    /**
     * Handles {@link AppendRequest} sent by leader.
     */
    void handleAppendRequest(AppendRequest request);

    /**
     * Handles {@link AppendSuccessResponse} for a previously sent append request by this node.
     */
    void handleAppendResponse(AppendSuccessResponse response);

    /**
     * Handles {@link AppendFailureResponse} for a previously sent append request by this node.
     */
    void handleAppendResponse(AppendFailureResponse response);

    /**
     * Handles {@link InstallSnapshot} sent by leader.
     */
    void handleInstallSnapshot(InstallSnapshot request);

    /**
     * Replicates the {@code RaftOperation} to the Raft group. Only leader can process replicate requests.
     * <p>
     * If operation is replicated and committed, then return value of {@link RaftOperation#doRun(long)}
     * is set as a result to the future.
     * <p>
     * Otherwise, if this node is not leader, or leader is demoted before committing the operation,
     * returned future is notified with a related exception.
     *
     * @param operation RaftOperation to replicate
     * @return future to get notified about result of the replication
     */
    ICompletableFuture replicate(RaftOperation operation);

    /**
     * Replicates the membership change to the Raft group. Only leader can process membership change requests.
     * <p>
     * If this node is not leader, or leader is demoted before committing the operation,
     * or membership change is not committed for any reason, then returned future is notified with a related exception.
     *
     * @param member member to add or remove
     * @param change type of membership change
     * @return future to get notified about result of the membership change
     */
    ICompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeType change);

    /**
     * Replicates the membership change to the Raft group, if expected members commit index is equal to the actual
     * one stored in Raft state. Otherwise fails with {@link com.hazelcast.raft.exception.MismatchingGroupMembersCommitIndexException}.
     * <p>
     * For more info see {@link #replicate(RaftOperation)}.
     *
     * @param member                  member to add or remove
     * @param change                  type of membership change
     * @param groupMembersCommitIndex expected members commit index
     * @return future to get notified about result of the membership change
     */
    ICompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeType change, long groupMembersCommitIndex);

    /**
     * Executes given {@code RaftOperation} on Raft group depending on the {@link QueryPolicy}
     * and returns the return value of {@link RaftOperation#doRun(long)} if operation is executed successfully.
     *
     * @param operation RaftOperation to query
     * @param queryPolicy query policy to decide where to execute operation
     * @return future to get notified about result of the query
     */
    ICompletableFuture query(RaftOperation operation, QueryPolicy queryPolicy);

}
