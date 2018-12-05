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

package com.hazelcast.cp.internal.raft.impl.task;

import com.hazelcast.core.Endpoint;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.MembershipChangeType;
import com.hazelcast.cp.internal.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.cp.internal.raft.exception.MemberDoesNotExistException;
import com.hazelcast.cp.internal.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.command.ApplyRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.logging.ILogger;

import java.util.Collection;
import java.util.LinkedHashSet;

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
 * {@link ApplyRaftGroupMembersCmd} Raft operation is created with members
 * according to the member parameter and membership change and it's replicated
 * via {@link ReplicateTask}.
 *
 * @see MembershipChangeType
 */
public class MembershipChangeTask implements Runnable {
    private final RaftNodeImpl raftNode;
    private final Long groupMembersCommitIndex;
    private final Endpoint member;
    private final MembershipChangeType changeType;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public MembershipChangeTask(RaftNodeImpl raftNode, SimpleCompletableFuture resultFuture, Endpoint member,
                                MembershipChangeType changeType) {
        this(raftNode, resultFuture, member, changeType, null);
    }

    public MembershipChangeTask(RaftNodeImpl raftNode, SimpleCompletableFuture resultFuture, Endpoint member,
                                MembershipChangeType changeType, Long groupMembersCommitIndex) {
        if (changeType == null) {
            throw new IllegalArgumentException("Null membership change type");
        }
        this.raftNode = raftNode;
        this.groupMembersCommitIndex = groupMembersCommitIndex;
        this.member = member;
        this.changeType = changeType;
        this.resultFuture = resultFuture;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        if (!verifyRaftNodeStatus()) {
            return;
        }

        RaftState state = raftNode.state();
        if (state.role() != LEADER) {
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), state.leader()));
            return;
        }

        if (!isValidGroupMemberCommitIndex()) {
            return;
        }

        Collection<Endpoint> members = new LinkedHashSet<Endpoint>(state.members());
        boolean memberExists = members.contains(member);

        switch (changeType) {
            case ADD:
                if (memberExists) {
                    resultFuture.setResult(new MemberAlreadyExistsException(member));
                    return;
                }
                members.add(member);
                break;

            case REMOVE:
                if (!memberExists) {
                    resultFuture.setResult(new MemberDoesNotExistException(member));
                    return;
                }
                members.remove(member);
                break;

            default:
                resultFuture.setResult(new IllegalArgumentException("Unknown type: " + changeType));
                return;
        }

        logger.info("New members after " + changeType + " " + member + " -> " + members);
        new ReplicateTask(raftNode, new ApplyRaftGroupMembersCmd(members, member, changeType), resultFuture).run();
    }

    private boolean verifyRaftNodeStatus() {
        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            resultFuture.setResult(new CPGroupDestroyedException(raftNode.getGroupId()));
            logger.severe("Cannot " + changeType + " " + member + " with expected members commit index: "
                    + groupMembersCommitIndex + " since raft node is terminated.");
            return false;
        } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
            logger.severe("Cannot " + changeType + " " + member + " with expected members commit index: "
                    + groupMembersCommitIndex + " since raft node is stepped down.");
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), null));
            return false;
        }

        return true;
    }

    private boolean isValidGroupMemberCommitIndex() {
        if (groupMembersCommitIndex != null) {
            RaftState state = raftNode.state();
            RaftGroupMembers groupMembers = state.committedGroupMembers();
            if (groupMembers.index() != groupMembersCommitIndex) {
                logger.severe("Cannot " + changeType + " " + member + " because expected members commit index: "
                        + groupMembersCommitIndex + " is different than group members commit index: " + groupMembers.index());

                Exception e = new MismatchingGroupMembersCommitIndexException(groupMembers.index(), groupMembers.members());
                resultFuture.setResult(e);
                return false;
            }
        }
        return true;
    }
}
