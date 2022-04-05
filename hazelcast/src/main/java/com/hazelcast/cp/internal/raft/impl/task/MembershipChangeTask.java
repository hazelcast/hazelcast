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

package com.hazelcast.cp.internal.raft.impl.task;

import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.cp.internal.raft.exception.MemberDoesNotExistException;
import com.hazelcast.cp.internal.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.UUID;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;

/**
 * MembershipChangeTask is executed to add/remove a member to the Raft group.
 * <p>
 * If membership change type is ADD but the member already exists in the group,
 * then future is notified with {@link MemberAlreadyExistsException}.
 * <p>
 * If membership change type is REMOVE but the member doesn't exist
 * in the group, then future is notified with
 * {@link MemberDoesNotExistException}.
 * <p>
 * {@link UpdateRaftGroupMembersCmd} Raft operation is created with members
 * according to the member parameter and membership change and it's replicated
 * via {@link ReplicateTask}.
 *
 * @see MembershipChangeMode
 */
public class MembershipChangeTask implements Runnable {
    private final RaftNodeImpl raftNode;
    private final Long groupMembersCommitIndex;
    private final RaftEndpoint member;
    private final MembershipChangeMode membershipChangeMode;
    private final InternalCompletableFuture resultFuture;
    private final ILogger logger;

    public MembershipChangeTask(RaftNodeImpl raftNode, InternalCompletableFuture resultFuture, RaftEndpoint member,
                                MembershipChangeMode membershipChangeMode) {
        this(raftNode, resultFuture, member, membershipChangeMode, null);
    }

    public MembershipChangeTask(RaftNodeImpl raftNode, InternalCompletableFuture resultFuture, RaftEndpoint member,
                                MembershipChangeMode membershipChangeMode, Long groupMembersCommitIndex) {
        if (membershipChangeMode == null) {
            throw new IllegalArgumentException("Null membership change type");
        }
        this.raftNode = raftNode;
        this.groupMembersCommitIndex = groupMembersCommitIndex;
        this.member = member;
        this.membershipChangeMode = membershipChangeMode;
        this.resultFuture = resultFuture;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        try {
            if (!verifyRaftNodeStatus()) {
                return;
            }

            RaftState state = raftNode.state();
            if (state.role() != LEADER) {
                resultFuture.completeExceptionally(
                        new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), state.leader()));
                return;
            }

            if (!isValidGroupMemberCommitIndex()) {
                return;
            }

            Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>(state.members());
            boolean memberExists = members.contains(member);

            switch (membershipChangeMode) {
                case ADD:
                    if (memberExists) {
                        resultFuture.completeExceptionally(new MemberAlreadyExistsException(member));
                        return;
                    }
                    members.add(member);
                    break;

                case REMOVE:
                    if (!memberExists) {
                        resultFuture.completeExceptionally(new MemberDoesNotExistException(member));
                        return;
                    }
                    members.remove(member);
                    break;

                default:
                    resultFuture.completeExceptionally(new IllegalArgumentException("Unknown type: " + membershipChangeMode));
                    return;
            }

            logger.info("New members after " + membershipChangeMode + " " + member + " -> " + members);
            new ReplicateTask(raftNode, new UpdateRaftGroupMembersCmd(members, member, membershipChangeMode), resultFuture).run();
        } catch (Throwable t) {
            logger.severe(this + " failed", t);
            RaftEndpoint leader = raftNode.getLeader();
            UUID leaderUuid = leader != null ? leader.getUuid() : null;
            resultFuture.completeExceptionally(new CPSubsystemException("Internal failure", t, leaderUuid));
        }
    }

    private boolean verifyRaftNodeStatus() {
        switch (raftNode.getStatus()) {
            case INITIAL:
                resultFuture.completeExceptionally(new CannotReplicateException(null));
                return false;
            case TERMINATED:
            case STEPPED_DOWN:
                resultFuture.completeExceptionally(
                        new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), null));
                return false;
            default:
                return true;
        }
    }

    private boolean isValidGroupMemberCommitIndex() {
        if (groupMembersCommitIndex != null) {
            RaftState state = raftNode.state();
            RaftGroupMembers groupMembers = state.committedGroupMembers();
            if (groupMembers.index() != groupMembersCommitIndex) {
                logger.severe("Cannot " + membershipChangeMode + " " + member + " because expected members commit index: "
                        + groupMembersCommitIndex + " is different than group members commit index: " + groupMembers.index());

                Exception e = new MismatchingGroupMembersCommitIndexException(groupMembers.index(), groupMembers.members());
                resultFuture.completeExceptionally(e);
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "MembershipChangeTask{" + "groupMembersCommitIndex=" + groupMembersCommitIndex + ", member=" + member
                + ", membershipChangeMode=" + membershipChangeMode + '}';
    }
}
