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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.MembershipChangeSchedule.CPGroupMembershipChange;
import com.hazelcast.cp.internal.operation.GetLeadedGroupsOp;
import com.hazelcast.cp.internal.operation.TransferLeadershipOp;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raftop.metadata.CompleteDestroyRaftGroupsOp;
import com.hazelcast.cp.internal.raftop.metadata.CompleteRaftGroupMembershipChangesOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetDestroyingRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetMembershipChangeScheduleOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cp.internal.RaftService.CP_SUBSYSTEM_MANAGEMENT_EXECUTOR;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Realizes pending Raft group membership changes and periodically checks
 * validity of local {@link RaftNode} instances on CP members
 */
class RaftGroupMembershipManager {

    static final long MANAGEMENT_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);
    static final HazelcastProperty LEADERSHIP_BALANCE_TASK_PERIOD
            = new HazelcastProperty("hazelcast.raft.leadership.rebalance.period", 60);
    private static final long CHECK_LOCAL_RAFT_NODES_TASK_PERIOD = 10;

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final RaftInvocationManager invocationManager;
    private final AtomicBoolean initialized = new AtomicBoolean();

    RaftGroupMembershipManager(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
        this.invocationManager = raftService.getInvocationManager();
    }

    void init() {
        if (raftService.getLocalCPMember() == null || !initialized.compareAndSet(false, true)) {
            return;
        }

        ExecutionService executionService = nodeEngine.getExecutionService();
        // scheduleWithRepetition skips subsequent execution if one is already running.
        executionService.scheduleWithRepetition(CP_SUBSYSTEM_MANAGEMENT_EXECUTOR, new RaftGroupDestroyHandlerTask(),
                MANAGEMENT_TASK_PERIOD_IN_MILLIS, MANAGEMENT_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(CP_SUBSYSTEM_MANAGEMENT_EXECUTOR, new RaftGroupMembershipChangeHandlerTask(),
                MANAGEMENT_TASK_PERIOD_IN_MILLIS, MANAGEMENT_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(CP_SUBSYSTEM_MANAGEMENT_EXECUTOR, new CheckLocalRaftNodesTask(),
                CHECK_LOCAL_RAFT_NODES_TASK_PERIOD, CHECK_LOCAL_RAFT_NODES_TASK_PERIOD, SECONDS);
        int leadershipRebalancePeriod = nodeEngine.getProperties().getInteger(LEADERSHIP_BALANCE_TASK_PERIOD);
        executionService.scheduleWithRepetition(CP_SUBSYSTEM_MANAGEMENT_EXECUTOR, new RaftGroupLeadershipBalanceTask(),
                leadershipRebalancePeriod, leadershipRebalancePeriod, SECONDS);
    }

    private boolean skipRunningTask() {
        return !(raftService.isDiscoveryCompleted() && raftService.isStartCompleted()
                && raftService.getMetadataGroupManager().isMetadataGroupLeader());
    }

    void rebalanceGroupLeaderships() {
        new RaftGroupLeadershipBalanceTask().run();
    }

    private class CheckLocalRaftNodesTask implements Runnable {

        public void run() {
            if (!(raftService.isDiscoveryCompleted() && raftService.isStartCompleted())) {
                return;
            }

            for (RaftNode raftNode : raftService.getAllRaftNodes()) {
                CPGroupId groupId = raftNode.getGroupId();
                if (groupId.equals(raftService.getMetadataGroupId())) {
                    continue;
                }

                if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
                    raftService.terminateRaftNode(groupId, false);
                    continue;
                } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
                    raftService.stepDownRaftNode(groupId);
                    continue;
                }

                CompletableFuture<CPGroupSummary> f = queryMetadata(new GetRaftGroupOp(groupId));

                f.whenCompleteAsync((group, t) -> {
                    if (t == null) {
                        if (group == null) {
                            logger.severe("Could not find CP group for local raft node of " + groupId);
                        } else if (group.status() == CPGroupStatus.DESTROYED) {
                            raftService.terminateRaftNode(groupId, true);
                        }
                    } else {
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

            commitDestroyedRaftGroups(destroyedGroupIds);
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
            return f.joinInternal();
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

        private void commitDestroyedRaftGroups(Set<CPGroupId> destroyedGroupIds) {
            RaftOp op = new CompleteDestroyRaftGroupsOp(destroyedGroupIds);
            CPGroupId metadataGroupId = raftService.getMetadataGroupId();
            Future<Collection<CPGroupId>> f = invocationManager.invoke(metadataGroupId, op);

            try {
                f.get();
                logger.info("Terminated CP groups: " + destroyedGroupIds + " are committed.");
            } catch (Exception e) {
                logger.severe("Cannot commit terminated CP groups: " + destroyedGroupIds, e);
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
            Map<CPGroupId, BiTuple<Long, Long>> changedGroups = new ConcurrentHashMap<>();

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
            return f.joinInternal();
        }

        private void applyOnRaftGroup(CountDownLatch latch, Map<CPGroupId, BiTuple<Long, Long>> changedGroups,
                                      CPGroupMembershipChange change) {
            CompletableFuture<Long> future;
            if (change.getMemberToRemove() != null) {
                future = invocationManager.changeMembership(change.getGroupId(), change.getMembersCommitIndex(),
                        change.getMemberToRemove(), MembershipChangeMode.REMOVE);
            } else {
                future = CompletableFuture.completedFuture(change.getMembersCommitIndex());
            }

            future.whenCompleteAsync((removeCommitIndex, t) -> {
                if (t == null) {
                    if (change.getMemberToAdd() != null) {
                        addMember(latch, changedGroups, change, removeCommitIndex);
                    } else {
                        changedGroups.put(change.getGroupId(), BiTuple.of(change.getMembersCommitIndex(), removeCommitIndex));
                        latch.countDown();
                    }
                } else {
                    long commitIndex = checkMemberRemoveCommitIndex(changedGroups, change, t);
                    if (commitIndex != NA_MEMBERS_COMMIT_INDEX) {
                        if (change.getMemberToAdd() != null) {
                            addMember(latch, changedGroups, change, commitIndex);
                        } else {
                            changedGroups.put(change.getGroupId(), BiTuple.of(change.getMembersCommitIndex(), commitIndex));
                            latch.countDown();
                        }
                    } else {
                        latch.countDown();
                    }
                }
            });
        }

        private void addMember(CountDownLatch latch, Map<CPGroupId, BiTuple<Long, Long>> changedGroups,
                               CPGroupMembershipChange change, long currentCommitIndex) {
            CompletableFuture<Long> future = invocationManager.changeMembership(change.getGroupId(), currentCommitIndex,
                    change.getMemberToAdd(), MembershipChangeMode.ADD);
            future.whenCompleteAsync((addCommitIndex, t) -> {
                if (t == null) {
                    changedGroups.put(change.getGroupId(), BiTuple.of(change.getMembersCommitIndex(), addCommitIndex));
                    latch.countDown();
                } else {
                    checkMemberAddCommitIndex(changedGroups, change, t);
                    latch.countDown();
                }
            });
        }

        private void checkMemberAddCommitIndex(Map<CPGroupId, BiTuple<Long, Long>> changedGroups, CPGroupMembershipChange change,
                                               Throwable t) {
            RaftEndpoint memberToAdd = change.getMemberToAdd();
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

                for (RaftEndpoint member : change.getMembers()) {
                    if (!member.equals(change.getMemberToRemove()) && !m.getMembers().contains(member)) {
                        logger.severe(msg);
                        return;
                    }
                }

                changedGroups.put(change.getGroupId(), BiTuple.of(change.getMembersCommitIndex(), m.getCommitIndex()));
                return;
            }

            logger.severe("Cannot get MEMBER ADD result of " + memberToAdd + " to " + change.getGroupId()
                    + " with members commit index: " + change.getMembersCommitIndex(), t);
        }

        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        private long checkMemberRemoveCommitIndex(Map<CPGroupId, BiTuple<Long, Long>> changedGroups,
                                                  CPGroupMembershipChange change, Throwable t) {
            RaftEndpoint removedMember = change.getMemberToRemove();
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

                    for (RaftEndpoint member : change.getMembers()) {
                        // Other group members except the removed one must be still present...
                        if (!member.equals(removedMember) && !m.getMembers().contains(member)) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }
                    }

                    // both member-remove and member-add are done.
                    changedGroups.put(change.getGroupId(), BiTuple.of(change.getMembersCommitIndex(), m.getCommitIndex()));
                    return NA_MEMBERS_COMMIT_INDEX;
                } else if (m.getMembers().size() != (change.getMembers().size() - 1)) {
                    // if there is no added member, I expect number of the learnt group members to be 1 less than
                    // the current members I know
                    logger.severe(msg);
                    return NA_MEMBERS_COMMIT_INDEX;
                }

                for (RaftEndpoint member : change.getMembers()) {
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

        private void completeMembershipChanges(Map<CPGroupId, BiTuple<Long, Long>> changedGroups) {
            RaftOp op = new CompleteRaftGroupMembershipChangesOp(changedGroups);
            CPGroupId metadataGroupId = raftService.getMetadataGroupId();
            CompletableFuture<Object> future = invocationManager.invoke(metadataGroupId, op);

            try {
                future.get();
            } catch (Exception e) {
                logger.severe("Cannot commit CP group membership changes: " + changedGroups, e);
            }
        }
    }

    private final class RaftGroupLeadershipBalanceTask implements Runnable {

        @Override
        public void run() {
            if (skipRunningTask()) {
                return;
            }

            try {
                rebalanceLeaderships();
            } catch (Exception e) {
                if (logger.isFineEnabled()) {
                    logger.warning("Cannot execute leadership rebalance at the moment", e);
                } else {
                    logger.info("Cannot execute leadership rebalance at the moment: "
                            + e.getClass().getName() + ": " + e.getMessage());
                }
            }
        }

        private void rebalanceLeaderships() {
            Map<RaftEndpoint, CPMember> members = getMembers();
            Collection<CPGroupId> groupIds = getCpGroupIds();

            final int avgGroupsPerMember = groupIds.size() / members.size();
            final boolean overAvgAllowed = groupIds.size() % members.size() != 0;

            Collection<CPGroupSummary> allGroups = new ArrayList<>(groupIds.size());
            for (CPGroupId groupId : groupIds) {
                CPGroupSummary group = getCpGroup(groupId);
                allGroups.add(group);
            }

            logger.fine("Searching for leadership imbalance in " + groupIds.size() + " CPGroups, "
                    + "average groups per member is " + avgGroupsPerMember);

            Set<CPMember> handledMembers = new HashSet<>(members.size());
            Map<CPMember, Collection<CPGroupId>> leaderships = getLeadershipsMap(members);

            for (; ;) {
                BiTuple<CPMember, Integer> from = getEndpointWithMaxLeaderships(leaderships, avgGroupsPerMember, handledMembers);
                if (from.element1 == null) {
                    // nothing to transfer
                    logger.info("CPGroup leadership balance is fine, cannot rebalance further...");
                    return;
                }

                logger.info("Searching a candidate transfer leadership from "
                        + from.element1 + " with " + from.element2 + " leaderships.");
                Collection<CPGroupSummary> groups = getLeaderGroupsOf(from.element1, leaderships.get(from.element1), allGroups);

                int maxLeaderships;
                if (overAvgAllowed) {
                    // - When from-member has more than (avg + 1), then leadership can be transferred to any member
                    // which has leaderships equal to or less than avg.
                    // - When from-member has equal to (avg + 1), then leadership can be transferred to only members
                    // which have leaderships less than avg.
                    if (from.element2 > avgGroupsPerMember + 1) {
                        maxLeaderships = avgGroupsPerMember;
                    } else {
                        maxLeaderships = avgGroupsPerMember - 1;
                    }
                } else {
                    maxLeaderships = avgGroupsPerMember;
                }
                BiTuple<CPMember, CPGroupId> to = getEndpointWithMinLeaderships(groups, leaderships, maxLeaderships);
                if (to.element1 == null) {
                    logger.info("No candidate could be found to get leadership from " + from.element1 + ". Skipping to next...");
                    // could not found target member to transfer membership
                    // try to find next leader
                    handledMembers.add(from.element1);
                    continue;
                }

                if (!transferLeadership(from.element1, to.element1, to.element2)) {
                    // could not transfer leadership
                    // try next time
                    return;
                }
                leaderships = getLeadershipsMap(members);
            }
        }

        private Map<CPMember, Collection<CPGroupId>> getLeadershipsMap(Map<RaftEndpoint, CPMember> members) {
            Map<CPMember, Collection<CPGroupId>> leaderships = new HashMap<>();
            OperationService operationService = nodeEngine.getOperationService();
            StringBuilder s = new StringBuilder("Current leadership claims:");
            for (CPMember member : members.values()) {
                Collection<CPGroupId> groups =
                        operationService.<Collection<CPGroupId>>invokeOnTarget(null, new GetLeadedGroupsOp(),
                                member.getAddress()).join();
                leaderships.put(member, groups);
                if (logger.isFineEnabled()) {
                    logger.fine(member + " claims it's leader of " + groups.size() + " groups: " + groups);
                }
                s.append('\n').append('\t').append(member).append(" has ").append(groups.size()).append(",");
            }
            s.setLength(s.length() - 1);
            s.append(" leaderships.");
            logger.info(s.toString());
            return leaderships;
        }

        private Collection<CPGroupSummary> getLeaderGroupsOf(CPMember member, Collection<CPGroupId> leaderships,
                Collection<CPGroupSummary> groups) {
            List<CPGroupSummary> memberGroups = new ArrayList<>();
            for (CPGroupSummary group : groups) {
                if (CPGroup.METADATA_CP_GROUP_NAME.equals(group.id().getName())) {
                    // ignore metadata group, we don't expect any significant load on metadata
                    continue;
                }
                if (!leaderships.contains(group.id())) {
                    continue;
                }
                if (group.members().contains(member)) {
                    memberGroups.add(group);
                }
            }
            return memberGroups;
        }

        private BiTuple<CPMember, CPGroupId> getEndpointWithMinLeaderships(Collection<CPGroupSummary> groups,
                                                                          Map<CPMember, Collection<CPGroupId>> leaderships,
                                                                          int maxLeaderships) {
            CPMember to = null;
            CPGroupId groupId = null;
            int min = maxLeaderships;
            for (CPGroupSummary group : groups) {
                for (CPMember member : group.members()) {
                    Collection<CPGroupId> g = leaderships.get(member);
                    int k = g != null ? g.size() : 0;
                    if (k < min) {
                        min = k;
                        to = member;
                        groupId = group.id();
                    }
                }
            }
            return BiTuple.of(to, groupId);
        }

        private BiTuple<CPMember, Integer> getEndpointWithMaxLeaderships(Map<CPMember, Collection<CPGroupId>> leaderships,
                                                                         int minLeaderships, Set<CPMember> excludeSet) {
            CPMember from = null;
            int max = minLeaderships;
            for (Entry<CPMember, Collection<CPGroupId>> entry : leaderships.entrySet()) {
                if (excludeSet.contains(entry.getKey())) {
                    continue;
                }
                int count = entry.getValue().size();
                if (count > max) {
                    from = entry.getKey();
                    max = count;
                }
            }
            return BiTuple.of(from, max);
        }

        private boolean transferLeadership(CPMember from, CPMember to, CPGroupId groupId) {
            logger.info("Transferring leadership from " + from + " to " + to + " in " + groupId);
            try {
                nodeEngine.getOperationService()
                        .invokeOnTarget(null, new TransferLeadershipOp(groupId, to), from.getAddress())
                        .join();
                return true;
            } catch (Exception e) {
                logger.warning(e);
            }
            return false;
        }

        private Map<RaftEndpoint, CPMember> getMembers() {
            InternalCompletableFuture<Collection<CPMemberInfo>> future = queryMetadata(new GetActiveCPMembersOp());
            Collection<CPMemberInfo> members = future.join();
            Map<RaftEndpoint, CPMember> map = new HashMap<>(members.size());
            for (CPMemberInfo member : members) {
                map.put(member.toRaftEndpoint(), member);
            }
            return map;
        }

        private CPGroupSummary getCpGroup(CPGroupId groupId) {
            InternalCompletableFuture<CPGroupSummary> f = queryMetadata(new GetRaftGroupOp(groupId));
            return f.join();
        }

        private Collection<CPGroupId> getCpGroupIds() {
            InternalCompletableFuture<Collection<CPGroupId>> future = queryMetadata(new GetActiveRaftGroupIdsOp());
            Collection<CPGroupId> groupIds = future.join();
            groupIds.remove(raftService.getMetadataGroupId());
            return groupIds;
        }
    }

    private <T> InternalCompletableFuture<T> queryMetadata(RaftOp op) {
        return invocationManager.query(raftService.getMetadataGroupId(), op, LEADER_LOCAL);
    }
}
