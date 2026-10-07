/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPGroupsSnapshot;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raftop.metadata.PublishCPGroupInfoOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.cp.internal.RaftService.CP_SUBSYSTEM_EXECUTOR;

/**
 * Tracks a view of CP group information from 1 cluster member's perspective. This relies
 * on AP-style {@link PublishCPGroupInfoOp} propagation from CP group leaders to publish
 * their own group changes - as such, this tracker maintains a <b>best effort<b/>
 * view that is not guaranteed to be up-to-date.
 */
public class CPGroupViewTracker {
    static final HazelcastProperty LEADER_BROADCAST_TASK_PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.raft.leadership.broadcast.period.seconds", 30);

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;

    // maintains a "last known" view of group membership states
    private final Map<CPGroupId, GroupState> groupIdToState = new ConcurrentHashMap<>();

    // Publishing invocation counter, used for debugging and testing
    private final AtomicLong totalPublications = new AtomicLong();

    CPGroupViewTracker(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());

        // Periodically re-publish our state when we're the leader of any CP groups - this helps ensure
        //  eventual consistency of this information, in case a previous broadcast operation failed
        int leaderBroadcastTaskIntervalSeconds = nodeEngine.getProperties().getInteger(LEADER_BROADCAST_TASK_PERIOD_SECONDS);
        logger.fine("Scheduling leader broadcast task with interval %d (s)", leaderBroadcastTaskIntervalSeconds);
        nodeEngine.getExecutionService().scheduleWithRepetition(CP_SUBSYSTEM_EXECUTOR, new LeaderBroadcastTask(),
                leaderBroadcastTaskIntervalSeconds, leaderBroadcastTaskIntervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Callback to update the last known leader of a CP Group, usually called
     * when a {@link com.hazelcast.cp.exception.NotLeaderException} is received
     * and a new leader is provided.
     *
     * @param groupId  the {@link CPGroupId} to update the known leader for
     * @param cpLeader the {@link CPMember} of the new known leader of this group
     */
    public void setLastKnownLeader(CPGroupId groupId, @Nullable CPMember cpLeader) {
        if (cpLeader == null) {
            // treat as though we have lost track of the group, wait for publication by new leader to update
            groupIdToState.remove(groupId);
            return;
        }

        // Provide a temporary entry if there's no existing state, or if the provided leader does not match our records
        groupIdToState.compute(groupId, (id, existingState) -> {
            if (existingState == null || existingState.leader == null || !existingState.leader.equals(cpLeader)) {
                // provide a temporary view of the leader only for now, wait for publication by leader to update
                return new GroupState(cpLeader, Collections.emptySet(), -1);
            }
            // the leader provided matches our records, no further action
            return existingState;
        });
    }

    /**
     * Retrieves the {@link CPMember} of the last known leader for the
     * provided {@link CPGroupId}.
     *
     * @param groupId the ID of the group to look up the leader of
     * @return        the {@link CPMember} of leader of this group if known, else {@code null}
     */
    public CPMember getLastKnownLeader(CPGroupId groupId) {
        GroupState state = groupIdToState.get(groupId);
        return state == null ? null : state.leader;
    }

    /**
     * Retrieves the last known {@link com.hazelcast.cp.CPGroupsSnapshot.GroupInfo}
     * for the provided {@link CPGroupId} if it exists, or {@code null} otherwise.
     * @param groupId the {@link CPGroupId} to retrieve information for
     * @return        the {@link com.hazelcast.cp.CPGroupsSnapshot.GroupInfo} if
     *                available, or {@code null} otherwise.
     */
    @Nullable
    public CPGroupsSnapshot.GroupInfo getGroupInfo(CPGroupId groupId) {
        GroupState state = groupIdToState.get(groupId);
        return state == null ? null : state.toGroupInfo();
    }

    /**
     * Internal callback used to update the membership view when a CP member is removed.
     *
     * @param member the {@link CPMember} of the member being removed
     */
    public void onCPMemberRemoved(CPMember member) {
        groupIdToState.entrySet().removeIf(entry -> {
            if (member.equals(entry.getValue().leader)) {
                // remove entire group state and wait for new leader to update
                return true;
            }
            entry.getValue().followers.remove(member);
            return false;
        });
    }

    /**
     * Internal callback used to update the membership view when a CP group is destroyed.
     *
     * @param groupId the ID of the group that has been destroyed
     */
    public void onCPGroupDestroyed(CPGroupId groupId) {
        GroupState state = groupIdToState.remove(groupId);
        // only the leader of the group should broadcast this change to other members
        if (state != null && state.leader != null && state.leader.equals(raftService.getLocalCPMember())) {
            publishGroupLeadershipStates();
        }
    }

    /**
     * Fetches the latest {@link RaftNode} information for all nodes led by this member
     * and publishes them to all members using the {@link PublishCPGroupInfoOp}, including
     * itself.
     */
    @SuppressWarnings("checkstyle:NPathComplexity")
    public void publishGroupLeadershipStates() {
        RaftEndpoint localCPEndpoint = raftService.getLocalCPEndpoint();
        if (localCPEndpoint == null) {
            logger.fine("Unable to publish local CP group view state as the local CP endpoint is null");
            return;
        }

        Collection<CPGroupId> leadedGroups = raftService.getLeadedGroups();
        Map<CPGroupId, Set<CPMember>> followersMap = new HashMap<>(leadedGroups.size());
        Map<CPGroupId, Integer> groupToTerm = new HashMap<>(leadedGroups.size());
        for (CPGroupId groupId : leadedGroups) {
            RaftNode raftNode = raftService.getRaftNode(groupId);
            if (raftNode == null) {
                logger.fine("Unable to retrieve local RaftNode information for led group: %s", groupId);
                continue;
            }
            if (raftService.isRaftGroupDestroyedOrTerminated(groupId)) {
                // It's possible our led groups is slightly out of date
                continue;
            }

            Collection<RaftEndpoint> appliedMembers = raftNode.getAppliedMembers();
            Set<CPMember> followers = new HashSet<>(appliedMembers.size() - 1);
            groupToTerm.put(groupId, ((RaftNodeImpl) raftNode).state().term());
            for (RaftEndpoint endpoint : appliedMembers) {
                if (endpoint.equals(localCPEndpoint)) {
                    continue;
                }
                CPMember cpMember = raftService.getInvocationManager().getCPMember(endpoint);
                if (cpMember != null) {
                    followers.add(cpMember);
                }
            }
            followersMap.put(groupId, followers);
        }

        // Send the update even if our followersMap is empty, as this could indicate group removals

        // Prepare operation and send to all cluster members, including the local member
        Set<Member> clusterMembers = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();

        CPMember localMember = raftService.getLocalCPMember();
        for (Member member : clusterMembers) {
            PublishCPGroupInfoOp op = new PublishCPGroupInfoOp(localMember, followersMap, groupToTerm);
            operationService.executeOrSend(RaftService.SERVICE_NAME, op, member.getAddress());
        }
        totalPublications.getAndIncrement();
    }

    /**
     * Internal callback to receive updates from {@link PublishCPGroupInfoOp}.
     * This method also invokes an update to be sent to listening clients if
     * data has changed on this member.
     *
     * @param cpLeader              {@link CPMember} object for the group leader
     * @param followerInfo          mapping of CP group ID to a {@link Set} of follower UUIDs
     * @param groupTerms            mapping of CP group ID to the provided term of that group
     */
    public void receiveUpdateForLedGroups(CPMember cpLeader, Map<CPGroupId, Set<CPMember>> followerInfo,
                                          Map<CPGroupId, Integer> groupTerms) {
        AtomicBoolean changed = new AtomicBoolean();
        for (Map.Entry<CPGroupId, Set<CPMember>> entry : followerInfo.entrySet()) {
            // the group may have been destroyed/terminated since the update was sent
            if (raftService.isRaftGroupDestroyedOrTerminated(entry.getKey())) {
                continue;
            }
            int term = groupTerms.getOrDefault(entry.getKey(), 0);
            groupIdToState.compute(entry.getKey(), (id, currentState) -> {
                if (currentState == null || currentState.term <= term) {
                    changed.set(true);
                    return new GroupState(cpLeader, entry.getValue(), term);
                }
                return currentState;
            });
        }

        // Remove groups that were led by this member, but not included in the update
        if (groupIdToState.entrySet().removeIf(entry ->
                !followerInfo.containsKey(entry.getKey()) && Objects.equals(entry.getValue().leader, cpLeader))) {
            changed.set(true);
        }

        // Send updates to clients if data changed
        if (changed.get()) {
            nodeEngine.getNode().getClientEngine().getCPGroupViewListenerService().onGroupViewChange();
        }
    }

    /**
     * Internal callback to receive initial CP group information when joining a cluster.
     *
     * @param cpGroupsSnapshot the {@link CPGroupsSnapshot} to base our view on
     */
    public void receivePreJoinOp(CPGroupsSnapshot cpGroupsSnapshot) {
        reset();

        Map<CPGroupId, CPGroupsSnapshot.GroupInfo> infoMap = cpGroupsSnapshot.getAllGroupInformation();
        for (Map.Entry<CPGroupId, CPGroupsSnapshot.GroupInfo> entry : infoMap.entrySet()) {
            // the group may have been destroyed/terminated since the operation was received
            if (raftService.isRaftGroupDestroyedOrTerminated(entry.getKey())) {
                continue;
            }
            this.groupIdToState.put(entry.getKey(), new GroupState(entry.getValue()));
        }
    }

    /**
     * Creates an immutable snapshot of this member's current CP group information view.
     *
     * @param includeUuidMapping if {@code true}, this {@link CPGroupsSnapshot} will include a
     *                           mapping of CP UUIDs to AP UUIDs for all members.
     * @return a new {@link CPGroupsSnapshot} of this member's current CP group information view
     */
    public CPGroupsSnapshot createSnapshotView(boolean includeUuidMapping) {
        Map<CPGroupId, CPGroupsSnapshot.GroupInfo> newMapping = new HashMap<>(groupIdToState.size());
        Map<UUID, UUID> cpToApUuids = includeUuidMapping ? new HashMap<>() : Collections.emptyMap();
        for (Map.Entry<CPGroupId, GroupState> entry : groupIdToState.entrySet()) {
            if (includeUuidMapping) {
                Member apLeader = raftService.getClusterMember(entry.getValue().leader);
                if (apLeader == null) {
                    // don't include in snapshot if we can't find the AP leader
                    continue;
                }
                cpToApUuids.put(entry.getValue().leader.getUuid(), apLeader.getUuid());
            }
            newMapping.put(entry.getKey(), entry.getValue().toGroupInfo());
            if (includeUuidMapping) {
                // include follower UUID mapping - we don't need to abort the snapshot if we can't find them, only leader matters
                for (CPMember follower : entry.getValue().followers) {
                    Member apMember = raftService.getClusterMember(follower);
                    if (apMember != null) {
                        cpToApUuids.put(follower.getUuid(), apMember.getUuid());
                    }
                }
            }
        }
        return new CPGroupsSnapshot(newMapping, cpToApUuids);
    }

    /**
     * Clears all currently tracked CP group information.
     */
    public void reset() {
        logger.fine("Resetting known CP group view information");
        groupIdToState.clear();
    }

    // Package accessible for testing
    void broadcastStateIfLeader() {
        // broadcast state of any currently led groups, and also broadcast if we have leadership for
        //  any local data, regardless of whether it's current (we may need to update about removal)
        if (!raftService.getLeadedGroups().isEmpty() || isLeaderOfAnyLocal()) {
            logger.fine("Broadcasting leadership state from periodic task");
            publishGroupLeadershipStates();
        } else {
            logger.fine("Not broadcasting leadership state as we are not a leader!");
        }
    }

    private boolean isLeaderOfAnyLocal() {
        CPMemberInfo localCPMember = raftService.getLocalCPMember();
        return groupIdToState.values().stream().anyMatch(state -> state.leader != null && state.leader.equals(localCPMember));
    }

    // Used for testing
    long getTotalPublications() {
        return totalPublications.get();
    }

    /**
     * Internal, mutable variant of {@link CPGroupsSnapshot.GroupInfo}
     */
    private static final class GroupState {
        private final Set<CPMember> followers;
        private final int term;
        private final CPMember leader;

        GroupState(CPMember leader, Set<CPMember> followers, int term) {
            this.leader = leader;
            this.followers = followers;
            this.term = term;
        }

        GroupState(CPGroupsSnapshot.GroupInfo groupInfo) {
            this.leader = groupInfo.leader();
            this.followers = groupInfo.followers();
            this.term = groupInfo.term();
        }

        public CPGroupsSnapshot.GroupInfo toGroupInfo() {
            return new CPGroupsSnapshot.GroupInfo(leader, followers, term);
        }

        @Override
        public String toString() {
            return "GroupState {term=" + term + ", "
                    + "leader=" + leader + ", "
                    + "followers=" + followers + "}";
        }
    }

    private final class LeaderBroadcastTask implements Runnable {
        @Override
        public void run() {
            broadcastStateIfLeader();
        }
    }
}
