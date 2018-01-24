package com.hazelcast.raft.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.raft.exception.MemberDoesNotExistException;
import com.hazelcast.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.command.ApplyRaftGroupMembersCmd;
import com.hazelcast.raft.impl.state.RaftGroupMembers;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * MembershipChangeTask is executed to add/remove a member to the Raft group.
 * <p>
 * If membership change type is ADD but the member already exists in the group,
 * then future is notified with {@link MemberAlreadyExistsException}.
 * <p>
 * If membership change type is REMOVE but the member doesn't exist in the group,
 * then future is notified with {@link MemberDoesNotExistException}.
 * <p>
 * {@link ApplyRaftGroupMembersCmd} Raft operation is created with members according to the member parameter
 * and membership change and it's replicated via {@link ReplicateTask}.
 *
 * @see MembershipChangeType
 */
public class MembershipChangeTask implements Runnable {
    private final RaftNodeImpl raftNode;
    private final Long groupMembersCommitIndex;
    private final RaftEndpoint member;
    private final MembershipChangeType changeType;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public MembershipChangeTask(RaftNodeImpl raftNode, SimpleCompletableFuture resultFuture, RaftEndpoint member,
                                MembershipChangeType changeType) {
        this(raftNode, resultFuture, member, changeType, null);
    }

    public MembershipChangeTask(RaftNodeImpl raftNode, SimpleCompletableFuture resultFuture, RaftEndpoint member,
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
        if (!isValidGroupMemberCommitIndex()) {
            return;
        }

        logger.info("Changing membership -> " + changeType + ": " + member);

        RaftState state = raftNode.state();
        Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>(state.members());
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
        logger.info("New members after " + changeType + " -> " + members);
        new ReplicateTask(raftNode, new ApplyRaftGroupMembersCmd(members), resultFuture).run();
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
