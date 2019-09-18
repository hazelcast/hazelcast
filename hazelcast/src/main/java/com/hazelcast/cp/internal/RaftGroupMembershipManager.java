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

package com.hazelcast.cp.internal;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.MembershipChangeSchedule.CPGroupMembershipChange;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raftop.metadata.CompleteDestroyRaftGroupsOp;
import com.hazelcast.cp.internal.raftop.metadata.CompleteRaftGroupMembershipChangesOp;
import com.hazelcast.cp.internal.raftop.metadata.DestroyRaftNodesOp;
import com.hazelcast.cp.internal.raftop.metadata.GetDestroyingRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetMembershipChangeScheduleOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.internal.util.SimpleCompletedFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.util.ExceptionUtil.peel;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Realizes pending Raft group membership changes and periodically checks
 * validity of local {@link RaftNode} instances on CP members
 */
class RaftGroupMembershipManager {

    static final long MANAGEMENT_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);
    private static final long CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(10);

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private volatile RaftInvocationManager invocationManager;

    RaftGroupMembershipManager(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
    }

    void init() {
        if (raftService.getLocalCPMember() == null) {
            return;
        }

        this.invocationManager = raftService.getInvocationManager();

        ExecutionService executionService = nodeEngine.getExecutionService();
        // scheduleWithRepetition skips subsequent execution if one is already running.
        executionService.scheduleWithRepetition(new RaftGroupDestroyHandlerTask(), MANAGEMENT_TASK_PERIOD_IN_MILLIS,
                MANAGEMENT_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new RaftGroupMembershipChangeHandlerTask(), MANAGEMENT_TASK_PERIOD_IN_MILLIS,
                MANAGEMENT_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new CheckLocalRaftNodesTask(), CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS,
                CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
    }

    private boolean skipRunningTask() {
        return !raftService.getMetadataGroupManager().isMetadataGroupLeader();
    }

    private class CheckLocalRaftNodesTask implements Runnable {

        public void run() {
            for (RaftNode raftNode : raftService.getAllRaftNodes()) {
                CPGroupId groupId = raftNode.getGroupId();
                if (groupId.equals(raftService.getMetadataGroupId())) {
                    continue;
                }

                if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
                    raftService.destroyRaftNode(groupId);
                    continue;
                } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
                    raftService.stepDownRaftNode(groupId);
                    continue;
                }

                ICompletableFuture<CPGroupInfo> f = queryMetadata(new GetRaftGroupOp(groupId));

                f.andThen(new ExecutionCallback<CPGroupInfo>() {
                    @Override
                    public void onResponse(CPGroupInfo group) {
                        if (group == null) {
                            logger.severe("Could not find CP group for local raft node of " + groupId);
                        } else if (group.status() == CPGroupStatus.DESTROYED) {
                            raftService.destroyRaftNode(groupId);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.warning("Could not get CP group info of " + groupId, t);
                    }
                });
            }
        }

    }

    private class RaftGroupDestroyHandlerTask implements Runnable {
        @Override
        public void run() {
            if (skipRunningTask()) {
                return;
            }

            Set<CPGroupId> destroyedGroupIds = destroyRaftGroups();
            if (destroyedGroupIds.isEmpty()) {
                return;
            }

            if (!commitDestroyedRaftGroups(destroyedGroupIds)) {
                return;
            }

            for (CPGroupId groupId : destroyedGroupIds) {
                raftService.destroyRaftNode(groupId);
            }

            OperationService operationService = nodeEngine.getOperationService();
            for (CPMemberInfo member : raftService.getMetadataGroupManager().getActiveMembers()) {
                if (!member.equals(raftService.getLocalCPMember())) {
                    operationService.send(new DestroyRaftNodesOp(destroyedGroupIds), member.getAddress());
                }
            }
        }

        private Set<CPGroupId> destroyRaftGroups() {
            Collection<CPGroupId> destroyingRaftGroupIds = getDestroyingRaftGroupIds();
            if (destroyingRaftGroupIds.isEmpty()) {
                return Collections.emptySet();
            }

            Map<CPGroupId, Future<Object>> futures = new HashMap<>();
            for (CPGroupId groupId : destroyingRaftGroupIds) {
                Future<Object> future = invocationManager.destroy(groupId);
                futures.put(groupId, future);
            }

            Set<CPGroupId> destroyedGroupIds = new HashSet<>();
            for (Entry<CPGroupId, Future<Object>> e : futures.entrySet()) {
                if (isRaftGroupDestroyed(e.getKey(), e.getValue())) {
                    destroyedGroupIds.add(e.getKey());
                }
            }

            return destroyedGroupIds;
        }

        private Collection<CPGroupId> getDestroyingRaftGroupIds() {
            InternalCompletableFuture<Collection<CPGroupId>> f = queryMetadata(new GetDestroyingRaftGroupIdsOp());
            return f.join();
        }

        private boolean isRaftGroupDestroyed(CPGroupId groupId, Future<Object> future) {
            try {
                future.get();
                return true;
            }  catch (InterruptedException e) {
                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);
                return false;
            } catch (ExecutionException e) {
                if (peel(e) instanceof CPGroupDestroyedException) {
                    return true;
                }

                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);

                return false;
            }
        }

        private boolean commitDestroyedRaftGroups(Set<CPGroupId> destroyedGroupIds) {
            RaftOp op = new CompleteDestroyRaftGroupsOp(destroyedGroupIds);
            CPGroupId metadataGroupId = raftService.getMetadataGroupId();
            Future<Collection<CPGroupId>> f = invocationManager.invoke(metadataGroupId, op);

            try {
                f.get();
                logger.info("Terminated CP groups: " + destroyedGroupIds + " are committed.");
                return true;
            } catch (Exception e) {
                logger.severe("Cannot commit terminated CP groups: " + destroyedGroupIds, e);
                return false;
            }
        }
    }

    private class RaftGroupMembershipChangeHandlerTask implements Runnable {

        private static final int NA_MEMBERS_COMMIT_INDEX = -1;

        @Override
        public void run() {
            if (skipRunningTask()) {
                return;
            }

            MembershipChangeSchedule schedule = getMembershipChangeSchedule();
            if (schedule == null) {
                return;
            }

            if (logger.isFineEnabled()) {
                logger.fine("Handling " + schedule);
            }

            List<CPGroupMembershipChange> changes = schedule.getChanges();
            CountDownLatch latch = new CountDownLatch(changes.size());
            Map<CPGroupId, Tuple2<Long, Long>> changedGroups = new ConcurrentHashMap<>();

            for (CPGroupMembershipChange change : changes) {
                applyOnRaftGroup(latch, changedGroups, change);
            }

            try {
                latch.await();
                completeMembershipChanges(changedGroups);
            } catch (InterruptedException e) {
                logger.warning("Membership changes interrupted while executing " + schedule
                        + ". completed: " + changedGroups, e);
                Thread.currentThread().interrupt();
            }
        }

        private MembershipChangeSchedule getMembershipChangeSchedule() {
            InternalCompletableFuture<MembershipChangeSchedule> f = queryMetadata(new GetMembershipChangeScheduleOp());
            return f.join();
        }

        private void applyOnRaftGroup(CountDownLatch latch, Map<CPGroupId, Tuple2<Long, Long>> changedGroups,
                                      CPGroupMembershipChange change) {
            ICompletableFuture<Long> future;
            if (change.getMemberToRemove() != null) {
                future = invocationManager.changeMembership(change.getGroupId(), change.getMembersCommitIndex(),
                        change.getMemberToRemove(), MembershipChangeMode.REMOVE);
            } else {
                future = new SimpleCompletedFuture<>(change.getMembersCommitIndex());
            }

            future.andThen(new ExecutionCallback<Long>() {
                @Override
                public void onResponse(Long removeCommitIndex) {
                    if (change.getMemberToAdd() != null) {
                        addMember(latch, changedGroups, change, removeCommitIndex);
                    } else {
                        changedGroups.put(change.getGroupId(), Tuple2.of(change.getMembersCommitIndex(), removeCommitIndex));
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    long removeCommitIndex = checkMemberRemoveCommitIndex(changedGroups, change, t);
                    if (removeCommitIndex != NA_MEMBERS_COMMIT_INDEX) {
                        onResponse(removeCommitIndex);
                    } else {
                        latch.countDown();
                    }
                }
            });
        }

        private void addMember(CountDownLatch latch, Map<CPGroupId, Tuple2<Long, Long>> changedGroups,
                               CPGroupMembershipChange change, long currentCommitIndex) {
            ICompletableFuture<Long> future = invocationManager.changeMembership(change.getGroupId(), currentCommitIndex,
                    change.getMemberToAdd(), MembershipChangeMode.ADD);
            future.andThen(new ExecutionCallback<Long>() {
                @Override
                public void onResponse(Long addCommitIndex) {
                    changedGroups.put(change.getGroupId(), Tuple2.of(change.getMembersCommitIndex(), addCommitIndex));
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    checkMemberAddCommitIndex(changedGroups, change, t);
                    latch.countDown();
                }
            });
        }

        private void checkMemberAddCommitIndex(Map<CPGroupId, Tuple2<Long, Long>> changedGroups, CPGroupMembershipChange change,
                                               Throwable t) {
            CPMemberInfo memberToAdd = change.getMemberToAdd();
            if (t instanceof MismatchingGroupMembersCommitIndexException) {
                MismatchingGroupMembersCommitIndexException m = (MismatchingGroupMembersCommitIndexException) t;
                String msg = "MEMBER ADD commit of " + change + " failed. Actual group members: " + m.getMembers()
                        + " with commit index: " + m.getCommitIndex();

                // learnt group members must contain the added member and the current members I know

                if (!m.getMembers().contains(memberToAdd)) {
                    logger.severe(msg);
                    return;
                }

                if (change.getMemberToRemove() != null) {
                    // I expect the member to be removed to be removed from the group
                    if (m.getMembers().contains(change.getMemberToRemove())) {
                        logger.severe(msg);
                        return;
                    }

                    // I know the removed member has left the group and the added member has joined.
                    // So member counts must be same...
                    if (m.getMembers().size() != change.getMembers().size()) {
                        logger.severe(msg);
                        return;
                    }
                } else if (m.getMembers().size() != (change.getMembers().size() + 1)) {
                    // if there is no removed member, I expect number of the learnt number of group members to be 1 greater than
                    // the current members I know
                    logger.severe(msg);
                    return;
                }

                for (CPMemberInfo member : change.getMembers()) {
                    if (!member.equals(change.getMemberToRemove()) && !m.getMembers().contains(member)) {
                        logger.severe(msg);
                        return;
                    }
                }

                changedGroups.put(change.getGroupId(), Tuple2.of(change.getMembersCommitIndex(), m.getCommitIndex()));
                return;
            }

            logger.severe("Cannot get MEMBER ADD result of " + memberToAdd + " to " + change.getGroupId()
                    + " with members commit index: " + change.getMembersCommitIndex(), t);
        }

        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        private long checkMemberRemoveCommitIndex(Map<CPGroupId, Tuple2<Long, Long>> changedGroups,
                                                  CPGroupMembershipChange change, Throwable t) {
            CPMemberInfo removedMember = change.getMemberToRemove();
            if (t instanceof MismatchingGroupMembersCommitIndexException) {
                MismatchingGroupMembersCommitIndexException m = (MismatchingGroupMembersCommitIndexException) t;
                String msg = "MEMBER REMOVE commit of " + change + " failed. Actual group members: " + m.getMembers()
                        + " with commit index: " + m.getCommitIndex();

                if (m.getMembers().contains(removedMember)) {
                    logger.severe(msg);
                    return NA_MEMBERS_COMMIT_INDEX;
                }

                // The member to be added can be already joined to the group
                if (change.getMemberToAdd() != null && m.getMembers().contains(change.getMemberToAdd())) {
                    // I know the removed member has left the group and the member to be added has already joined to the group.
                    // So member sizes must be same...
                    if (m.getMembers().size() != change.getMembers().size()) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    for (CPMemberInfo member : change.getMembers()) {
                        // Other group members except the removed one must be still present...
                        if (!member.equals(removedMember) && !m.getMembers().contains(member)) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }
                    }

                    // both member-remove and member-add are done.
                    changedGroups.put(change.getGroupId(), Tuple2.of(change.getMembersCommitIndex(), m.getCommitIndex()));
                    return NA_MEMBERS_COMMIT_INDEX;
                } else if (m.getMembers().size() != (change.getMembers().size() - 1)) {
                    // if there is no added member, I expect number of the learnt group members to be 1 less than
                    // the current members I know
                    logger.severe(msg);
                    return NA_MEMBERS_COMMIT_INDEX;
                }

                for (CPMemberInfo member : change.getMembers()) {
                    // Other group members except the removed one must be still present...
                    if (!member.equals(removedMember) && !m.getMembers().contains(member)) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }
                }

                return m.getCommitIndex();
            }

            logger.severe("Cannot get MEMBER REMOVE result of " + removedMember + " to " + change.getGroupId(), t);
            return NA_MEMBERS_COMMIT_INDEX;
        }

        private void completeMembershipChanges(Map<CPGroupId, Tuple2<Long, Long>> changedGroups) {
            RaftOp op = new CompleteRaftGroupMembershipChangesOp(changedGroups);
            CPGroupId metadataGroupId = raftService.getMetadataGroupId();
            ICompletableFuture<Object> future = invocationManager.invoke(metadataGroupId, op);

            try {
                future.get();
            } catch (Exception e) {
                logger.severe("Cannot commit CP group membership changes: " + changedGroups, e);
            }
        }
    }

    private <T> InternalCompletableFuture<T> queryMetadata(RaftOp op) {
        return invocationManager.query(raftService.getMetadataGroupId(), op, LEADER_LOCAL);
    }
}
