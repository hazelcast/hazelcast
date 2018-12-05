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

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Member;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.exception.MetadataRaftGroupNotInitializedException;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftNodeOp;
import com.hazelcast.cp.internal.raftop.metadata.DestroyRaftNodesOp;
import com.hazelcast.cp.internal.raftop.metadata.InitializeMetadataRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.SendActiveCPMembersOp;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cp.CPGroup.CPGroupStatus.ACTIVE;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYED;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYING;
import static com.hazelcast.cp.internal.MembershipChangeContext.CPGroupMembershipChangeContext;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Maintains the CP subsystem metadata, such as CP groups, active CP members,
 * leaving and joining CP members, etc.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class MetadataRaftGroupManager implements SnapshotAwareService<MetadataRaftGroupSnapshot>  {

    public static final RaftGroupId INITIAL_METADATA_GROUP_ID = new RaftGroupId(CPGroup.METADATA_CP_GROUP_NAME, 0, 0);

    private static final long DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS = 1000;
    private static final long BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS = 10;

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final CPSubsystemConfig config;
    private final AtomicReference<CPMemberInfo> localMember = new AtomicReference<CPMemberInfo>();
    // groups are read outside of Raft
    private final ConcurrentMap<CPGroupId, CPGroupInfo> groups = new ConcurrentHashMap<CPGroupId, CPGroupInfo>();
    private volatile RaftGroupId metadataGroupId;
    // activeMembers must be an ordered non-null collection
    private volatile Collection<CPMemberInfo> activeMembers = Collections.emptySet();
    private final AtomicBoolean discoveryCompleted = new AtomicBoolean();
    private volatile Collection<CPMemberInfo> initialCPMembers;
    private volatile MembershipChangeContext membershipChangeContext;
    private volatile long groupIdSeed;

    MetadataRaftGroupManager(NodeEngine nodeEngine, RaftService raftService, CPSubsystemConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;
        this.metadataGroupId = INITIAL_METADATA_GROUP_ID;
    }

    boolean initLocalCPMemberOnStartup() {
        initLocalMember();

        boolean cpSubsystemEnabled = (config.getCPMemberCount() > 0);
        if (cpSubsystemEnabled) {
            scheduleDiscoveryInitialCPMembersTask();
        } else {
            disableDiscovery();
        }

        return cpSubsystemEnabled;
    }

    void initPromotedCPMember(CPMemberInfo member) {
        if (!localMember.compareAndSet(null, member)) {
            return;
        }

        scheduleRaftGroupMembershipManagementTasks();
    }

    private void scheduleRaftGroupMembershipManagementTasks() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new BroadcastActiveCPMembersTask(),
                BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS, BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS, SECONDS);

        RaftGroupMembershipManager membershipManager = new RaftGroupMembershipManager(nodeEngine, raftService);
        membershipManager.init();
    }

    private boolean initLocalMember() {
        // By default, we use the same member UUID for both AP and CP members.
        // But it's not guaranteed to be same. For example;
        // - During a split-brain merge, AP member UUID is renewed but CP member UUID remains the same.
        // - While promoting a member to CP when Hot Restart is enabled, CP member doesn't use the AP member's UUID
        // but instead generates a new UUID.
        return localMember.compareAndSet(null, new CPMemberInfo(nodeEngine.getLocalMember()));
    }

    void restart(long seed) {
        doSetActiveMembers(Collections.<CPMemberInfo>emptySet());
        groups.clear();
        initialCPMembers = null;
        localMember.set(null);
        groupIdSeed = seed;
        metadataGroupId = new RaftGroupId(CPGroup.METADATA_CP_GROUP_NAME, groupIdSeed, 0);

        if (config == null) {
            return;
        }
        discoveryCompleted.set(false);
        initLocalMember();
        membershipChangeContext = null;

        Runnable task = new DiscoverInitialCPMembersTask();
        nodeEngine.getExecutionService().schedule(task, DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    @Override
    public MetadataRaftGroupSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        if (!metadataGroupId.equals(groupId)) {
            return null;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Taking snapshot for commit-index: " + commitIndex);
        }

        MetadataRaftGroupSnapshot snapshot = new MetadataRaftGroupSnapshot();
        for (CPGroupInfo group : groups.values()) {
            snapshot.addRaftGroup(group);
        }
        for (CPMemberInfo member : activeMembers) {
            snapshot.addMember(member);
        }
        snapshot.setMembershipChangeContext(membershipChangeContext);
        snapshot.setGroupIdTerm(groupIdSeed);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, MetadataRaftGroupSnapshot snapshot) {
        ensureMetadataGroupId(groupId);
        checkNotNull(snapshot);

        if (logger.isFineEnabled()) {
            logger.fine("Restoring snapshot for commit-index: " + commitIndex);
        }

        for (CPGroupInfo group : snapshot.getGroups()) {
            CPGroupInfo existingGroup = groups.get(group.id());

            if (group.status() == ACTIVE && existingGroup == null) {
                createRaftGroup(group);
                continue;
            }

            if (group.status() == DESTROYING) {
                if (existingGroup == null) {
                    createRaftGroup(group);
                } else {
                    existingGroup.setDestroying();
                }
                continue;
            }

            if (group.status() == DESTROYED) {
                if (existingGroup == null) {
                    addRaftGroup(group);
                } else {
                    completeDestroyRaftGroup(existingGroup);
                }
            }
        }

        doSetActiveMembers(unmodifiableCollection(new LinkedHashSet<CPMemberInfo>(snapshot.getMembers())));

        membershipChangeContext = snapshot.getMembershipChangeContext();
        groupIdSeed = snapshot.getGroupIdTerm();
    }

    private void ensureMetadataGroupId(CPGroupId groupId) {
        checkTrue(metadataGroupId.equals(groupId), "Invalid RaftGroupId! Expected: " + metadataGroupId
                + ", Actual: " + groupId);
    }

    CPMemberInfo getLocalMember() {
        return localMember.get();
    }

    public CPGroupId getMetadataGroupId() {
        return metadataGroupId;
    }

    long getGroupIdSeed() {
        return groupIdSeed;
    }

    public Collection<CPGroupId> getGroupIds() {
        List<CPGroupId> groupIds = new ArrayList<CPGroupId>(groups.keySet());
        sort(groupIds, new CPGroupIdComparator());

        return groupIds;
    }

    public Collection<CPGroupId> getActiveGroupIds() {
        List<CPGroupId> activeGroupIds = new ArrayList<CPGroupId>(1);
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == ACTIVE) {
                activeGroupIds.add(group.id());
            }
        }

        sort(activeGroupIds, new CPGroupIdComparator());

        return activeGroupIds;
    }

    public CPGroupInfo getRaftGroup(CPGroupId groupId) {
        checkNotNull(groupId);

        if ((groupId instanceof RaftGroupId) && ((RaftGroupId) groupId).seed() < groupIdSeed) {
            throw new CPGroupDestroyedException(groupId);
        }

        return groups.get(groupId);
    }

    public CPGroupInfo getActiveRaftGroup(String groupName) {
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == CPGroupStatus.ACTIVE && group.name().equals(groupName)) {
                return group;
            }
        }

        return null;
    }

    public void initializeMetadataRaftGroup(List<CPMemberInfo> initialMembers, int metadataMembersCount,
                                            long expectedGroupIdSeed) {
        checkNotNull(initialMembers);
        checkTrue(metadataMembersCount > 1, "initial METADATA CP group must contain at least 2 members: "
                + metadataMembersCount);
        checkTrue(initialMembers.size() >= metadataMembersCount, "Initial CP members should contain all metadata members");

        if (initialCPMembers != null) {
            checkTrue(initialCPMembers.size() == initialMembers.size(), "Invalid initial CP members! Expected: "
                    + initialCPMembers + ", Actual: " + initialMembers);
            checkTrue(initialCPMembers.containsAll(initialMembers), "Invalid initial CP members! Expected: "
                    + initialCPMembers + ", Actual: " + initialMembers);
        }

        List<CPMemberInfo> metadataMembers = initialMembers.subList(0, metadataMembersCount);
        CPGroupInfo metadataGroup = new CPGroupInfo(metadataGroupId, metadataMembers);
        CPGroupInfo existingMetadataGroup = groups.putIfAbsent(metadataGroupId, metadataGroup);
        if (existingMetadataGroup != null) {
            checkTrue(metadataMembersCount == existingMetadataGroup.initialMemberCount(),
                    "Cannot create METADATA CP group with " + metadataMembersCount
                            + " because it already exists with a different member list: " + existingMetadataGroup);

            for (CPMemberInfo member : metadataMembers) {
                checkTrue(existingMetadataGroup.containsInitialMember(member),
                        "Cannot create METADATA CP group with " + metadataMembersCount
                        + " because it already exists with a different member list: " + existingMetadataGroup);
            }

            return;
        }

        checkTrue(groupIdSeed == expectedGroupIdSeed, "Cannot create METADATA CP group. Local groupId seed: "
                + groupIdSeed + ", expected groupId seed: " + expectedGroupIdSeed);

        initialCPMembers = initialMembers;

        if (logger.isFineEnabled()) {
            logger.fine("METADATA CP group is initialized: " + metadataGroup + " term: " + expectedGroupIdSeed);
        }
    }

    public CPGroupId createRaftGroup(String groupName, Collection<CPMemberInfo> members, long commitIndex) {
        checkFalse(metadataGroupId.name().equalsIgnoreCase(groupName), groupName + " is reserved for internal usage!");
        checkIfMetadataRaftGroupInitialized();

        // keep configuration on every metadata node
        CPGroupInfo group = getRaftGroupByName(groupName);
        if (group != null) {
            if (group.memberCount() == members.size()) {
                if (logger.isFineEnabled()) {
                    logger.fine("CP group " + groupName + " already exists.");
                }

                return group.id();
            }

            throw new IllegalStateException(group.getId() + " already exists with a different size: " + group.memberCount());
        }

        CPMemberInfo leavingMember = membershipChangeContext != null ? membershipChangeContext.getLeavingMember() : null;
        for (CPMemberInfo member : members) {
            if (member.equals(leavingMember) || !activeMembers.contains(member)) {
                throw new CannotCreateRaftGroupException("Cannot create CP group: " + groupName + " since " + member
                        + " is not active");
            }
        }

        return createRaftGroup(new CPGroupInfo(new RaftGroupId(groupName, groupIdSeed, commitIndex), members));
    }

    @SuppressWarnings("unchecked")
    private CPGroupId createRaftGroup(CPGroupInfo group) {
        addRaftGroup(group);

        logger.info("New " + group.id() + " is created with " + group.members());

        RaftGroupId groupId = group.id();
        if (group.containsMember(getLocalMember())) {
            raftService.createRaftNode(groupId, group.memberImpls());
        } else {
            // Broadcast group-info to non-metadata group members
            OperationService operationService = nodeEngine.getOperationService();
            CPGroupInfo metadataGroup = groups.get(metadataGroupId);
            for (CPMemberInfo member : group.memberImpls()) {
                if (!metadataGroup.containsMember(member)) {
                    Operation op = new CreateRaftNodeOp(group.id(), (Collection) group.initialMembers());
                    operationService.send(op, member.getAddress());
                }
            }
        }

        return groupId;
    }

    private void addRaftGroup(CPGroupInfo group) {
        CPGroupId groupId = group.id();
        checkState(!groups.containsKey(groupId), group + " already exists!");
        groups.put(groupId, group);
    }

    private CPGroupInfo getRaftGroupByName(String name) {
        for (CPGroupInfo group : groups.values()) {
            if (group.status() != DESTROYED && group.name().equals(name)) {
                return group;
            }
        }
        return null;
    }

    public void triggerDestroyRaftGroup(CPGroupId groupId) {
        checkNotNull(groupId);
        checkState(membershipChangeContext == null,
                "Cannot destroy " + groupId + " while there are ongoing CP membership changes!");

        CPGroupInfo group = groups.get(groupId);
        checkNotNull(group, "No CP group exists for " + groupId + " to destroy!");

        if (group.setDestroying()) {
            logger.info("Destroying " + groupId);
        } else if (logger.isFineEnabled()) {
            logger.fine(groupId + " is already " + group.status());
        }
    }

    public void completeDestroyRaftGroups(Set<CPGroupId> groupIds) {
        checkNotNull(groupIds);

        for (CPGroupId groupId : groupIds) {
            checkNotNull(groupId);

            CPGroupInfo group = groups.get(groupId);
            checkNotNull(group, groupId + " does not exist to complete destroy");

            completeDestroyRaftGroup(group);
        }
    }

    private void completeDestroyRaftGroup(CPGroupInfo group) {
        CPGroupId groupId = group.id();
        if (group.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            sendDestroyRaftNodeOps(group);
        } else if (logger.isFineEnabled()) {
            logger.fine(groupId + " is already destroyed.");
        }
    }

    public void forceDestroyRaftGroup(String groupName) {
        checkNotNull(groupName);
        checkFalse(metadataGroupId.name().equalsIgnoreCase(groupName), "Cannot force-destroy the METADATA CP group!");

        boolean found = false;

        for (CPGroupInfo group : groups.values()) {
            if (group.name().equals(groupName)) {
                if (group.forceSetDestroyed()) {
                    logger.info(group.id() + " is force-destroyed.");
                    sendDestroyRaftNodeOps(group);
                } else if (logger.isFineEnabled()) {
                    logger.fine(group.id() + " is already force-destroyed.");
                }

                found = true;
            }
        }

        if (!found) {
            throw new IllegalArgumentException("CP group with name: " + groupName + " does not exist to force-destroy!");
        }
    }

    private void sendDestroyRaftNodeOps(CPGroupInfo group) {
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new DestroyRaftNodesOp(Collections.<CPGroupId>singleton(group.id()));
        for (CPMemberInfo member : group.memberImpls())  {
            if (member.equals(getLocalMember())) {
                raftService.destroyRaftNode(group.id());
            } else {
                operationService.send(op, member.getAddress());
            }
        }
    }

    /**
     * this method is idempotent
     */
    public void triggerRemoveMember(CPMemberInfo leavingMember) {
        checkNotNull(leavingMember);
        checkIfMetadataRaftGroupInitialized();

        if (shouldRemoveMember(leavingMember)) {
            return;
        }

        if (activeMembers.size() <= 2) {
            logger.warning(leavingMember + " is directly removed as there are only " + activeMembers.size() + " CP members");
            removeActiveMember(leavingMember);
            return;
        }

        initializeMembershipChangeContextForLeavingMember(leavingMember);
    }

    private void initializeMembershipChangeContextForLeavingMember(CPMemberInfo leavingMember) {
        List<CPGroupId> leavingGroupIds = new ArrayList<CPGroupId>();
        List<CPGroupMembershipChangeContext> leavingGroups = new ArrayList<CPGroupMembershipChangeContext>();
        for (CPGroupInfo group : groups.values()) {
            CPGroupId groupId = group.id();
            if (!group.containsMember(leavingMember) || group.status() == DESTROYED) {
                continue;
            }

            CPMemberInfo substitute = findSubstitute(group);
            if (substitute != null) {
                leavingGroupIds.add(groupId);
                leavingGroups.add(new CPGroupMembershipChangeContext(groupId, group.getMembersCommitIndex(),
                        group.memberImpls(), substitute, leavingMember));
            } else {
                leavingGroupIds.add(groupId);
                leavingGroups.add(new CPGroupMembershipChangeContext(groupId, group.getMembersCommitIndex(),
                        group.memberImpls(), null, leavingMember));
            }
        }

        if (leavingGroups.isEmpty()) {
            if (logger.isFineEnabled()) {
                logger.fine("Removing " + leavingMember + " directly since it is not present in any CP group.");
            }
            removeActiveMember(leavingMember);
            return;
        }

        membershipChangeContext = new MembershipChangeContext(leavingMember, leavingGroups);
        if (logger.isFineEnabled()) {
            logger.info(leavingMember + " will be removed from from " + leavingGroups);
        } else {
            logger.info(leavingMember + " will be removed from from " + leavingGroupIds);
        }
    }

    private boolean shouldRemoveMember(CPMemberInfo leavingMember) {
        if (!activeMembers.contains(leavingMember)) {
            logger.warning("Not removing " + leavingMember + " since it is not an active CP members");
            return false;
        }

        if (membershipChangeContext != null) {
            if (leavingMember.equals(membershipChangeContext.getLeavingMember())) {
                if (logger.isFineEnabled()) {
                    logger.fine(leavingMember + " is already marked as leaving.");
                }
                return true;
            }

            throw new CannotRemoveCPMemberException("There is already an ongoing CP membership change process. "
                    + "Cannot process remove request of " + leavingMember);
        }

        return false;
    }

    private CPMemberInfo findSubstitute(CPGroupInfo group) {
        for (CPMemberInfo substitute : activeMembers) {
            if (activeMembers.contains(substitute) && !group.containsMember(substitute)) {
                return substitute;
            }
        }

        return null;
    }

    public MembershipChangeContext completeRaftGroupMembershipChanges(Map<CPGroupId, Tuple2<Long, Long>> changedGroups) {
        checkNotNull(changedGroups);
        checkState(membershipChangeContext != null, "Cannot apply CP membership changes: "
                + changedGroups + " since there is no membership change context!");

        for (CPGroupMembershipChangeContext ctx : membershipChangeContext.getChanges()) {
            CPGroupId groupId = ctx.getGroupId();
            CPGroupInfo group = groups.get(groupId);
            checkState(group != null, groupId + "not found in CP groups: " + groups.keySet()
                    + "to apply " + ctx);
            Tuple2<Long, Long> t = changedGroups.get(groupId);

            if (t == null) {
                if (group.status() == DESTROYED && !changedGroups.containsKey(groupId)) {
                    if (logger.isFineEnabled()) {
                        logger.warning(groupId + " is already destroyed so will skip: " + ctx);
                    }
                    changedGroups.put(groupId, Tuple2.of(0L, 0L));
                }
                continue;
            }

            applyMembershipChange(ctx, group, t.element1, t.element2);
        }

        membershipChangeContext = membershipChangeContext.excludeCompletedChanges(changedGroups.keySet());

        CPMemberInfo leavingMember = membershipChangeContext.getLeavingMember();
        if (checkSafeToRemove(leavingMember)) {
            checkState(membershipChangeContext.getChanges().isEmpty(), "Leaving " + leavingMember
                    + " is removed from all groups but there are still pending membership changes: "
                    + membershipChangeContext);
            logger.info(leavingMember + " is removed from the CP subsystem.");
            removeActiveMember(leavingMember);
            membershipChangeContext = null;
        } else if (membershipChangeContext.getChanges().isEmpty()) {
            logger.info("Rebalancing is completed.");
            membershipChangeContext = null;
        }

        return membershipChangeContext;
    }

    @SuppressWarnings("unchecked")
    private void applyMembershipChange(CPGroupMembershipChangeContext ctx, CPGroupInfo group,
                                       long expectedMembersCommitIndex, long newMembersCommitIndex) {
        CPMemberInfo addedMember = ctx.getMemberToAdd();
        CPMemberInfo removedMember = ctx.getMemberToRemove();

        if (group.applyMembershipChange(removedMember, addedMember, expectedMembersCommitIndex, newMembersCommitIndex)) {
            if (logger.isFineEnabled()) {
                logger.fine("Applied add-member: " + (addedMember != null ? addedMember : "-") + " and remove-member: "
                        + (removedMember != null ? removedMember : "-") + " in "  + group.id()
                        + " with new members commit index: " + newMembersCommitIndex);
            }
            if (getLocalMember().equals(addedMember)) {
                // we are the added member to the group, we can try to create the local raft node if not created already
                raftService.createRaftNode(group.id(), group.memberImpls());
            } else if (addedMember != null) {
                // publish group-info to the joining member
                Operation op = new CreateRaftNodeOp(group.id(), (Collection) group.initialMembers());
                nodeEngine.getOperationService().send(op, addedMember.getAddress());
            }
        } else {
            logger.severe("Could not apply add-member: " + (addedMember != null ? addedMember : "-")
                    + " and remove-member: " + (removedMember != null ? removedMember : "-") + " in "  + group
                    + " with new members commit index: " + newMembersCommitIndex + " expected members commit index: "
                    + expectedMembersCommitIndex + " known members commit index: " + group.getMembersCommitIndex());
        }
    }

    private boolean checkSafeToRemove(CPMemberInfo leavingMember) {
        if (leavingMember == null) {
            return false;
        }

        for (CPGroupInfo group : groups.values()) {
            if (group.containsMember(leavingMember)) {
                if (group.status() != DESTROYED) {
                    return false;
                } else if (logger.isFineEnabled()) {
                    logger.warning("Leaving " + leavingMember + " was in the destroyed " + group.id());
                }
            }
        }

        return true;
    }

    private List<CPGroupMembershipChangeContext> getGroupMembershipChangesForNewMember(CPMemberInfo newMember) {
        List<CPGroupMembershipChangeContext> changes = new ArrayList<CPGroupMembershipChangeContext>();
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == ACTIVE && group.initialMemberCount() > group.memberCount()) {
                checkState(!group.memberImpls().contains(newMember), group + " already contains: " + newMember);

                changes.add(new CPGroupMembershipChangeContext(group.id(), group.getMembersCommitIndex(), group.memberImpls(),
                        newMember, null));
            }
        }

        return changes;
    }

    public Collection<CPMemberInfo> getActiveMembers() {
        return activeMembers;
    }

    public void setActiveMembers(RaftGroupId metadataGroupId, Collection<CPMemberInfo> members) {
        if (!isDiscoveryCompleted()) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring received active CP members: " + members + " since discovery is in progress.");
            }
            return;
        }
        CPMemberInfo localMember = getLocalMember();
        if (localMember != null) {
            if (logger.isFineEnabled()) {
                logger.fine(localMember + " is already part of CP members!");
            }
            assert this.metadataGroupId.equals(metadataGroupId);
            assert members.contains(localMember);
            return;
        }
        checkNotNull(members);
        checkTrue(members.size() > 1, "active members must contain at least 2 members: " + members);

        if (logger.isFineEnabled()) {
            logger.fine("Setting active CP members: " + members + ", METADATA groupId: " + metadataGroupId);
        }
        this.metadataGroupId = metadataGroupId;
        doSetActiveMembers(unmodifiableCollection(new LinkedHashSet<CPMemberInfo>(members)));
    }

    private void updateInvocationManagerMembers(Collection<CPMemberInfo> members) {
        raftService.getInvocationManager().getRaftInvocationContext().setMembers(members);
    }

    public Collection<CPGroupId> getDestroyingGroupIds() {
        Collection<CPGroupId> groupIds = new ArrayList<CPGroupId>();
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == DESTROYING) {
                groupIds.add(group.id());
            }
        }
        return groupIds;
    }

    public MembershipChangeContext getMembershipChangeContext() {
        return membershipChangeContext;
    }

    boolean isMetadataGroupLeader() {
        CPMemberInfo member = getLocalMember();
        if (member == null) {
            return false;
        }
        RaftNode raftNode = raftService.getRaftNode(metadataGroupId);
        // even if the local leader information is stale, it is fine.
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown() && member.equals(raftNode.getLeader());
    }

    /**
     * this method is idempotent
     */
    public void addActiveMember(CPMemberInfo member) {
        checkNotNull(member);
        checkIfMetadataRaftGroupInitialized();

        if (activeMembers.contains(member)) {
            if (logger.isFineEnabled()) {
                logger.fine(member + " already exists.");
            }
            return;
        }

        checkState(membershipChangeContext == null,
                "Cannot rebalance CP groups because there is ongoing " + membershipChangeContext);

        Collection<CPMemberInfo> newMembers = new LinkedHashSet<CPMemberInfo>(activeMembers);
        newMembers.add(member);
        doSetActiveMembers(unmodifiableCollection(newMembers));
        logger.info("Added new " + member + ". New active CP members list: " + newMembers);

        List<CPGroupMembershipChangeContext> changes = getGroupMembershipChangesForNewMember(member);
        if (changes.size() > 0) {
            if (logger.isFineEnabled()) {
                logger.fine("CP group rebalancing is triggered for " + changes);
            }
            membershipChangeContext = new MembershipChangeContext(null, changes);
        }
    }

    private void removeActiveMember(CPMemberInfo member) {
        Collection<CPMemberInfo> newMembers = new LinkedHashSet<CPMemberInfo>(activeMembers);
        newMembers.remove(member);
        doSetActiveMembers(unmodifiableCollection(newMembers));
    }

    private void doSetActiveMembers(Collection<CPMemberInfo> members) {
        activeMembers = unmodifiableCollection(members);
        updateInvocationManagerMembers(members);
        raftService.updateMissingMembers();
        broadcastActiveMembers();
    }

    private void checkIfMetadataRaftGroupInitialized() {
        if (!groups.containsKey(metadataGroupId)) {
            throw new MetadataRaftGroupNotInitializedException();
        }
    }

    void broadcastActiveMembers() {
        if (getLocalMember() == null) {
            return;
        }
        Collection<CPMemberInfo> members = activeMembers;
        if (members.isEmpty()) {
            return;
        }

        Set<Member> clusterMembers = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new SendActiveCPMembersOp(metadataGroupId, members);
        for (Member member : clusterMembers) {
            if (member.localMember()) {
                continue;
            }
            operationService.send(op, member.getAddress());
        }
    }

    boolean isDiscoveryCompleted() {
        return discoveryCompleted.get();
    }

    public void disableDiscovery() {
        if (config.getCPMemberCount() > 0) {
            logger.info("Disabling discovery of initial CP members since it is already completed...");
        }
        if (discoveryCompleted.compareAndSet(false, true)) {
            localMember.set(null);
        }
    }

    private void scheduleDiscoveryInitialCPMembersTask() {
        Runnable task = new DiscoverInitialCPMembersTask();
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(task, DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    private class BroadcastActiveCPMembersTask implements Runnable {
        @Override
        public void run() {
            if (!isMetadataGroupLeader()) {
                return;
            }
            broadcastActiveMembers();
        }
    }

    private class DiscoverInitialCPMembersTask implements Runnable {

        private Collection<Member> latestMembers = Collections.emptySet();

        @Override
        public void run() {
            if (shouldSkipOrReschedule()) {
                return;
            }

            Collection<Member> members = nodeEngine.getClusterService().getMembers();
            for (Member member : latestMembers) {
                if (!members.contains(member)) {
                    logger.severe(member + " left the cluster while discovering initial CP members!");
                    terminateNode();
                    return;
                }
            }

            latestMembers = members;

            if (rescheduleIfCPMemberCountNotSatisfied(members)) {
                return;
            }

            List<CPMemberInfo> cpMembers = getInitialCPMembers(members);

            if (completeDiscoveryIfNotCPMember(cpMembers)) {
                return;
            }

            activeMembers = unmodifiableCollection(new LinkedHashSet<CPMemberInfo>(cpMembers));
            updateInvocationManagerMembers(activeMembers);

            if (!commitInitialMetadataRaftGroup(cpMembers)) {
                terminateNode();
                return;
            }

            logger.info("Initial CP members: " + activeMembers + ", local: " + getLocalMember());
            broadcastActiveMembers();
            discoveryCompleted.set(true);
            scheduleRaftGroupMembershipManagementTasks();
        }

        private boolean shouldSkipOrReschedule() {
            if (isDiscoveryCompleted()) {
                return true;
            }

            if (!nodeEngine.getClusterService().isJoined()) {
                scheduleDiscoveryInitialCPMembersTask();
                return true;
            }

            return false;
        }

        private boolean rescheduleIfCPMemberCountNotSatisfied(Collection<Member> members) {
            if (members.size() < config.getCPMemberCount()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Waiting for " + config.getCPMemberCount() + " CP members to join the cluster. "
                            + "Current CP member count: " + members.size());
                }

                scheduleDiscoveryInitialCPMembersTask();
                return true;
            }
            return false;
        }

        private boolean completeDiscoveryIfNotCPMember(List<CPMemberInfo> cpMembers) {
            if (!cpMembers.contains(getLocalMember())) {
                if (logger.isFineEnabled()) {
                    logger.fine("I am not an initial CP member! I'll serve as an AP member.");
                }

                localMember.set(null);
                disableDiscovery();
                return true;
            }

            return false;
        }

        @SuppressWarnings("unchecked")
        private boolean commitInitialMetadataRaftGroup(List<CPMemberInfo> initialCPMembers) {
            int metadataGroupSize = config.getGroupSize();
            List<CPMemberInfo> metadataMembers = initialCPMembers.subList(0, metadataGroupSize);
            try {
                if (metadataMembers.contains(getLocalMember())) {
                    raftService.createRaftNode(metadataGroupId, metadataMembers);
                }

                RaftOp op = new InitializeMetadataRaftGroupOp(initialCPMembers, metadataGroupSize, groupIdSeed);
                raftService.getInvocationManager().invoke(metadataGroupId, op).get();
                logger.info("METADATA CP group is created with " + metadataMembers);
            } catch (Exception e) {
                logger.severe("Could not create METADATA CP group with initial CP members: " + metadataMembers
                        + " and METADATA CP group members: " + metadataMembers, e);
                return false;
            }
            return true;
        }

        private void terminateNode() {
            ((NodeEngineImpl) nodeEngine).getNode().shutdown(true);
        }

        private List<CPMemberInfo> getInitialCPMembers(Collection<Member> members) {
            assert members.size() >= config.getCPMemberCount();
            List<Member> memberList = new ArrayList<Member>(members).subList(0, config.getCPMemberCount());
            List<CPMemberInfo> cpMembers = new ArrayList<CPMemberInfo>(config.getCPMemberCount());
            for (Member member : memberList) {
                // During initial discovery, it's guaranteed that AP and CP member UUIDs will be the same.
                cpMembers.add(new CPMemberInfo(member));
            }

            sort(cpMembers, new CPMemberComparator());
            return cpMembers;
        }
    }

    @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
    private static class CPMemberComparator implements Comparator<CPMemberInfo> {
        @Override
        public int compare(CPMemberInfo o1, CPMemberInfo o2) {
            return o1.getUuid().compareTo(o2.getUuid());
        }
    }

    @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
    private static class CPGroupIdComparator implements Comparator<CPGroupId> {
        @Override
        public int compare(CPGroupId o1, CPGroupId o2) {
            return Long.valueOf(o1.id()).compareTo(o2.id());
        }
    }
}
