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
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.exception.MetadataRaftGroupInitInProgressException;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftNodeOp;
import com.hazelcast.cp.internal.raftop.metadata.DestroyRaftNodesOp;
import com.hazelcast.cp.internal.raftop.metadata.InitMetadataRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.PublishActiveCPMembersOp;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
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
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.MembershipChangeSchedule.CPGroupMembershipChange;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Maintains the CP subsystem metadata, such as CP groups, active CP members,
 * leaving and joining CP members, etc.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class MetadataRaftGroupManager implements SnapshotAwareService<MetadataRaftGroupSnapshot>  {

    public static final RaftGroupId INITIAL_METADATA_GROUP_ID = new RaftGroupId(METADATA_CP_GROUP_NAME, 0, 0);

    enum MetadataRaftGroupInitStatus {
        IN_PROGRESS,
        FAILED,
        SUCCESSFUL
    }

    private static final long DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS = 1000;
    private static final long DISCOVER_INITIAL_CP_MEMBERS_TASK_LOGGING_DELAY_MILLIS = 5000;
    private static final long BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS = 10;

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final CPSubsystemConfig config;

    // these fields are related to the local CP member but they are not maintained within the Metadata CP group
    private final AtomicReference<CPMemberInfo> localCPMember = new AtomicReference<CPMemberInfo>();
    private final AtomicReference<RaftGroupId> metadataGroupIdRef = new AtomicReference<RaftGroupId>(INITIAL_METADATA_GROUP_ID);
    private final AtomicBoolean discoveryCompleted = new AtomicBoolean();

    // all fields below are state of the Metadata CP group and put into Metadata snapshot and reset while restarting...
    // these fields are accessed outside of Raft while restarting or local querying, etc.
    private final ConcurrentMap<CPGroupId, CPGroupInfo> groups = new ConcurrentHashMap<CPGroupId, CPGroupInfo>();
    // activeMembers must be an ordered non-null collection
    private volatile Collection<CPMemberInfo> activeMembers = Collections.emptySet();
    private volatile long activeMembersCommitIndex;
    private volatile List<CPMemberInfo> initialCPMembers;
    private volatile MembershipChangeSchedule membershipChangeSchedule;
    private volatile MetadataRaftGroupInitStatus initializationStatus = MetadataRaftGroupInitStatus.IN_PROGRESS;
    private final Set<CPMemberInfo> initializedCPMembers = newSetFromMap(new ConcurrentHashMap<CPMemberInfo, Boolean>());
    private final Set<Long> initializationCommitIndices = newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    MetadataRaftGroupManager(NodeEngine nodeEngine, RaftService raftService, CPSubsystemConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;
    }

    boolean init() {
        boolean cpSubsystemEnabled = (config.getCPMemberCount() > 0);
        if (cpSubsystemEnabled) {
            scheduleDiscoverInitialCPMembersTask(true);
        } else {
            disableDiscovery();
        }

        return cpSubsystemEnabled;
    }

    void initPromotedCPMember(CPMemberInfo member) {
        if (!localCPMember.compareAndSet(null, member)) {
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

    void restart(long seed) {
        // reset order:
        // 1. active members
        // 2. active members index
        // 3. metadata group id ref

        activeMembers = Collections.emptySet();
        activeMembersCommitIndex = 0;
        groups.clear();
        initialCPMembers = null;
        initializationStatus = MetadataRaftGroupInitStatus.IN_PROGRESS;
        initializedCPMembers.clear();
        initializationCommitIndices.clear();
        membershipChangeSchedule = null;

        metadataGroupIdRef.set(new RaftGroupId(METADATA_CP_GROUP_NAME, seed, 0));
        localCPMember.set(null);
        discoveryCompleted.set(false);

        scheduleDiscoverInitialCPMembersTask(false);
    }

    @Override
    public MetadataRaftGroupSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        if (!getMetadataGroupId().equals(groupId)) {
            return null;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Taking snapshot for commit-index: " + commitIndex);
        }

        MetadataRaftGroupSnapshot snapshot = new MetadataRaftGroupSnapshot();
        snapshot.setMembers(activeMembers);
        snapshot.setMembersCommitIndex(activeMembersCommitIndex);
        snapshot.setGroups(groups.values());
        snapshot.setMembershipChangeSchedule(membershipChangeSchedule);
        snapshot.setInitialCPMembers(initialCPMembers);
        snapshot.setInitializedCPMembers(initializedCPMembers);
        snapshot.setInitializationStatus(initializationStatus);
        snapshot.setInitializationCommitIndices(initializationCommitIndices);

        return snapshot;
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, MetadataRaftGroupSnapshot snapshot) {
        ensureMetadataGroupId(groupId);
        checkNotNull(snapshot);

        Set<CPGroupId> snapshotGroupIds = new HashSet<CPGroupId>();
        for (CPGroupInfo group : snapshot.getGroups()) {
            groups.put(group.id(), group);
            snapshotGroupIds.add(group.id());
        }

        Iterator<CPGroupId> it = groups.keySet().iterator();
        while (it.hasNext()) {
            if (!snapshotGroupIds.contains(it.next())) {
                it.remove();
            }
        }

        doSetActiveMembers(snapshot.getMembersCommitIndex(), new LinkedHashSet<CPMemberInfo>(snapshot.getMembers()));
        membershipChangeSchedule = snapshot.getMembershipChangeSchedule();
        initialCPMembers = snapshot.getInitialCPMembers();
        initializedCPMembers.clear();
        initializedCPMembers.addAll(snapshot.getInitializedCPMembers());
        initializationStatus = snapshot.getInitializationStatus();
        initializationCommitIndices.clear();
        initializationCommitIndices.addAll(snapshot.getInitializationCommitIndices());

        if (logger.isFineEnabled()) {
            logger.fine("Restored snapshot at commit-index: " + commitIndex);
        }
    }

    private void ensureMetadataGroupId(CPGroupId groupId) {
        CPGroupId metadataGroupId = getMetadataGroupId();
        checkTrue(metadataGroupId.equals(groupId), "Invalid RaftGroupId! Expected: " + metadataGroupId
                + ", Actual: " + groupId);
    }

    CPMemberInfo getLocalCPMember() {
        return localCPMember.get();
    }

    public RaftGroupId getMetadataGroupId() {
        return metadataGroupIdRef.get();
    }

    long getGroupIdSeed() {
        return getMetadataGroupId().seed();
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

    public CPGroupInfo getGroup(CPGroupId groupId) {
        checkNotNull(groupId);

        if ((groupId instanceof RaftGroupId) && ((RaftGroupId) groupId).seed() < getGroupIdSeed()) {
            throw new CPGroupDestroyedException(groupId);
        }

        return groups.get(groupId);
    }

    public CPGroupInfo getActiveGroup(String groupName) {
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == CPGroupStatus.ACTIVE && group.name().equals(groupName)) {
                return group;
            }
        }

        return null;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    public boolean initMetadataGroup(long commitIndex, CPMemberInfo callerCPMember, List<CPMemberInfo> discoveredCPMembers,
                                     long expectedGroupIdSeed) {
        checkNotNull(discoveredCPMembers);

        // Fail fast if CP subsystem initialization is already failed
        if (initializationStatus == MetadataRaftGroupInitStatus.FAILED) {
            String msg = callerCPMember + "committed CP member list: " + discoveredCPMembers
                    + " after CP subsystem discovery has already failed.";
            logger.severe(msg);
            throw new IllegalArgumentException(msg);
        }

        if (discoveredCPMembers.size() != config.getCPMemberCount()) {
            String msg = callerCPMember + "'s discovered CP member list: " + discoveredCPMembers + " must consist of "
                    + config.getCPMemberCount() + " CP members";
            failMetadataRaftGroupInitializationIfNotCompletedAndThrow(msg);
        }

        if (initialCPMembers != null
                && (initialCPMembers.size() != discoveredCPMembers.size()
                || !initialCPMembers.containsAll(discoveredCPMembers))) {
            String msg = "Invalid initial CP members! Expected: " + initialCPMembers + ", Actual: " + discoveredCPMembers;
            failMetadataRaftGroupInitializationIfNotCompletedAndThrow(msg);
        }

        long groupIdSeed = getGroupIdSeed();
        if (groupIdSeed != expectedGroupIdSeed) {
            String msg = "Cannot create METADATA CP group. Local groupId seed: " + groupIdSeed + ", expected groupId seed: "
                    + expectedGroupIdSeed;
            failMetadataRaftGroupInitializationIfNotCompletedAndThrow(msg);
        }

        List<CPMemberInfo> discoveredMetadataMembers = discoveredCPMembers.subList(0, config.getGroupSize());
        CPGroupInfo metadataGroup = new CPGroupInfo(getMetadataGroupId(), discoveredMetadataMembers);
        CPGroupInfo existingMetadataGroup = groups.putIfAbsent(getMetadataGroupId(), metadataGroup);
        if (existingMetadataGroup != null) {
            Collection<CPMember> metadataMembers = existingMetadataGroup.initialMembers();
            if (discoveredMetadataMembers.size() != metadataMembers.size()
                    || !metadataMembers.containsAll(discoveredMetadataMembers)) {
                String msg = "Cannot create METADATA CP group with " + config.getCPMemberCount()
                        + " because it already exists with a different member list: " + existingMetadataGroup;
                failMetadataRaftGroupInitializationIfNotCompletedAndThrow(msg);
            }
        }

        // callerCPMember is either setting initialCPMembers or its discoveredCPMembers is same with initialCPMembers

        if (initializationStatus == MetadataRaftGroupInitStatus.SUCCESSFUL) {
            // Initialization already completed ...
            return true;
        }

        initializationCommitIndices.add(commitIndex);

        if (!initializedCPMembers.add(callerCPMember)) {
            // this caller's initialization is already noted.
            // It is enough to save its commit index so that we will notify it when the init process is completed...
            return false;
        }

        logger.fine("METADATA " + metadataGroup + " initialization is committed for " + callerCPMember + " with seed: "
                + expectedGroupIdSeed + " and discovered CP members: " + discoveredCPMembers);

        if (initializedCPMembers.size() == config.getCPMemberCount()) {
            // All CP members have committed their initialization

            // remove this commit because we will return the response directly here
            initializationCommitIndices.remove(commitIndex);

            logger.fine("METADATA " + metadataGroup + " initialization is completed with: " + initializedCPMembers);

            initializationStatus = MetadataRaftGroupInitStatus.SUCCESSFUL;
            completeFutures(getMetadataGroupId(), initializationCommitIndices, null);
            initializedCPMembers.clear();
            initializationCommitIndices.clear();
            return true;
        }

        if (initialCPMembers != null) {
            // Already initialized the CP member list...
            return false;
        }

        Collection<CPMemberInfo> cpMembers = new LinkedHashSet<CPMemberInfo>(discoveredCPMembers);
        initialCPMembers = unmodifiableList(new ArrayList<CPMemberInfo>(cpMembers));
        doSetActiveMembers(commitIndex, cpMembers);

        return false;
    }

    private void failMetadataRaftGroupInitializationIfNotCompletedAndThrow(String error) {
        logger.severe(error);
        RuntimeException exception = new IllegalArgumentException(error);
        if (initializationStatus == MetadataRaftGroupInitStatus.IN_PROGRESS) {
            initializationStatus = MetadataRaftGroupInitStatus.FAILED;
            completeFutures(getMetadataGroupId(), initializationCommitIndices, exception);
            initializedCPMembers.clear();
            initializationCommitIndices.clear();
        }

        throw exception;
    }

    public CPGroupId createRaftGroup(String groupName, Collection<CPMemberInfo> members, long commitIndex) {
        checkFalse(METADATA_CP_GROUP_NAME.equalsIgnoreCase(groupName), groupName + " is reserved for internal usage!");
        checkMetadataGroupInitSuccessful();

        // keep configuration on every metadata node
        CPGroupInfo group = getRaftGroupByName(groupName);
        if (group != null) {
            if (group.memberCount() == members.size()) {
                if (logger.isFineEnabled()) {
                    logger.fine("CP group " + groupName + " already exists.");
                }

                return group.id();
            }

            String msg = group.getId() + " already exists with a different size: " + group.memberCount();
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        CPMemberInfo leavingMember = membershipChangeSchedule != null ? membershipChangeSchedule.getLeavingMember() : null;
        for (CPMemberInfo member : members) {
            if (member.equals(leavingMember) || !activeMembers.contains(member)) {
                String msg = "Cannot create CP group: " + groupName + " since " + member + " is not active";
                if (logger.isFineEnabled()) {
                    logger.fine(msg);
                }

                throw new CannotCreateRaftGroupException(msg);
            }
        }

        return createRaftGroup(new CPGroupInfo(new RaftGroupId(groupName, getGroupIdSeed(), commitIndex), members));
    }

    @SuppressWarnings("unchecked")
    private CPGroupId createRaftGroup(CPGroupInfo group) {
        addRaftGroup(group);

        logger.info("New " + group.id() + " is created with " + group.members());

        RaftGroupId groupId = group.id();
        if (group.containsMember(getLocalCPMember())) {
            raftService.createRaftNode(groupId, group.memberImpls());
        } else {
            // Broadcast group-info to non-metadata group members
            OperationService operationService = nodeEngine.getOperationService();
            CPGroupInfo metadataGroup = groups.get(getMetadataGroupId());
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
        if (groups.containsKey(groupId)) {
            String msg = group + " already exists!";
            if (logger.isFineEnabled()) {
                logger.warning(msg);
            }

            throw new IllegalStateException(msg);
        }

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
        checkMetadataGroupInitSuccessful();

        if (membershipChangeSchedule != null) {
            String msg = "Cannot destroy " + groupId + " while there are ongoing CP membership changes!";
            if (logger.isFineEnabled()) {
                logger.warning(msg);
            }

            throw new IllegalStateException(msg);
        }

        CPGroupInfo group = groups.get(groupId);
        if (group == null) {
            String msg = "No CP group exists for " + groupId + " to destroy!";
            if (logger.isFineEnabled()) {
                logger.warning(msg);
            }

            throw new IllegalArgumentException(msg);
        }

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
            if (!groups.containsKey(groupId)) {
                String msg = groupId + " does not exist to complete destroy";
                logger.warning(msg);
                throw new IllegalArgumentException(msg);
            }
        }

        for (CPGroupId groupId : groupIds) {
            completeDestroyRaftGroup(groups.get(groupId));
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
        checkFalse(METADATA_CP_GROUP_NAME.equalsIgnoreCase(groupName), "Cannot force-destroy the METADATA CP group!");
        checkMetadataGroupInitSuccessful();

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
            if (member.equals(getLocalCPMember())) {
                raftService.destroyRaftNode(group.id());
            } else {
                operationService.send(op, member.getAddress());
            }
        }
    }

    /**
     * this method is idempotent
     */
    public boolean removeMember(long commitIndex, CPMemberInfo leavingMember) {
        checkNotNull(leavingMember);
        checkMetadataGroupInitSuccessful();

        if (!activeMembers.contains(leavingMember)) {
            logger.fine("Not removing " + leavingMember + " since it is not an active CP member");
            return true;
        }

        if (membershipChangeSchedule != null) {
            if (leavingMember.equals(membershipChangeSchedule.getLeavingMember())) {
                membershipChangeSchedule = membershipChangeSchedule.addRetriedCommitIndex(commitIndex);

                if (logger.isFineEnabled()) {
                    logger.fine(leavingMember + " is already marked as leaving.");
                }

                return false;
            }

            String msg = "There is already an ongoing CP membership change process. " + "Cannot process remove request of "
                    + leavingMember;

            if (logger.isFineEnabled()) {
                logger.fine(msg);
            }

            throw new CannotRemoveCPMemberException(msg);
        }

        if (activeMembers.size() == 2) {
            // There are two CP members.
            // If this operation is committed, it means both CP members have appended this operation.
            // I am returning a retry response, so that leavingMember will retry and commit this operation again.
            // Commit of its retry will ensure that both CP members' activeMember.size() == 1,
            // so that they will complete their shutdown in RaftService.ensureCPMemberRemoved()
            logger.warning(leavingMember + " is directly removed as there are only " + activeMembers.size() + " CP members.");
            removeActiveMember(commitIndex, leavingMember);
            throw new RetryableHazelcastException();
        } else if (activeMembers.size() == 1) {
            // This is the last CP member. It is not removed from the active CP members list
            // so that it will complete its shutdown in RaftService.ensureCPMemberRemoved()
            logger.fine("Not removing the last active CP member: " + leavingMember + " to help it complete its shutdown");
            return true;
        }

        return initMembershipChangeScheduleForLeavingMember(commitIndex, leavingMember);
    }

    private boolean initMembershipChangeScheduleForLeavingMember(long commitIndex, CPMemberInfo leavingMember) {
        List<CPGroupId> leavingGroupIds = new ArrayList<CPGroupId>();
        List<CPGroupMembershipChange> changes = new ArrayList<CPGroupMembershipChange>();
        for (CPGroupInfo group : groups.values()) {
            CPGroupId groupId = group.id();
            if (!group.containsMember(leavingMember) || group.status() == DESTROYED) {
                continue;
            }

            CPMemberInfo substitute = findSubstitute(group);
            if (substitute != null) {
                leavingGroupIds.add(groupId);
                changes.add(new CPGroupMembershipChange(groupId, group.getMembersCommitIndex(),
                        group.memberImpls(), substitute, leavingMember));
            } else {
                leavingGroupIds.add(groupId);
                changes.add(new CPGroupMembershipChange(groupId, group.getMembersCommitIndex(),
                        group.memberImpls(), null, leavingMember));
            }
        }

        if (changes.isEmpty()) {
            if (logger.isFineEnabled()) {
                logger.fine("Removing " + leavingMember + " directly since it is not present in any CP group.");
            }
            removeActiveMember(commitIndex, leavingMember);
            return true;
        }

        membershipChangeSchedule = MembershipChangeSchedule.forLeavingMember(singletonList(commitIndex), leavingMember, changes);
        if (logger.isFineEnabled()) {
            logger.info(leavingMember + " will be removed from " + changes);
        } else {
            logger.info(leavingMember + " will be removed from " + leavingGroupIds);
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

    public MembershipChangeSchedule completeRaftGroupMembershipChanges(long commitIndex,
                                                                       Map<CPGroupId, Tuple2<Long, Long>> changedGroups) {
        checkNotNull(changedGroups);
        if (membershipChangeSchedule == null) {
            String msg = "Cannot apply CP membership changes: " + changedGroups + " since there is no membership change context!";
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }

        for (CPGroupMembershipChange change : membershipChangeSchedule.getChanges()) {
            CPGroupId groupId = change.getGroupId();
            CPGroupInfo group = groups.get(groupId);
            checkState(group != null, groupId + "not found in CP groups: " + groups.keySet()
                    + "to apply " + change);
            Tuple2<Long, Long> t = changedGroups.get(groupId);

            if (t != null) {
                if (!applyMembershipChange(change, group, t.element1, t.element2)) {
                    changedGroups.remove(groupId);
                }
            } else if (group.status() == DESTROYED && !changedGroups.containsKey(groupId)) {
                if (logger.isFineEnabled()) {
                    logger.warning(groupId + " is already destroyed so will skip: " + change);
                }
                changedGroups.put(groupId, Tuple2.of(0L, 0L));
            }
        }

        membershipChangeSchedule = membershipChangeSchedule.excludeCompletedChanges(changedGroups.keySet());

        if (checkSafeToRemoveIfCPMemberLeaving(membershipChangeSchedule)) {
            CPMemberInfo leavingMember = membershipChangeSchedule.getLeavingMember();
            removeActiveMember(commitIndex, leavingMember);
            completeFutures(getMetadataGroupId(), membershipChangeSchedule.getMembershipChangeCommitIndices(), null);
            membershipChangeSchedule = null;
            logger.info(leavingMember + " is removed from the CP subsystem.");

        } else if (membershipChangeSchedule.getChanges().isEmpty()) {
            completeFutures(getMetadataGroupId(), membershipChangeSchedule.getMembershipChangeCommitIndices(), null);
            membershipChangeSchedule = null;
            logger.info("Rebalancing is completed.");
        }

        return membershipChangeSchedule;
    }

    private void completeFutures(CPGroupId groupId, Collection<Long> indices, Object result) {
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            if (raftNode != null) {
                for (Long index : indices) {
                    raftNode.completeFuture(index, result);
                }
            } else {
                logger.severe("RaftNode not found for " + groupId + " to notify commit indices " + indices + " with " + result);
            }
        }
    }

    private boolean applyMembershipChange(CPGroupMembershipChange change, CPGroupInfo group,
                                          long expectedMembersCommitIndex, long newMembersCommitIndex) {
        CPMemberInfo addedMember = change.getMemberToAdd();
        CPMemberInfo removedMember = change.getMemberToRemove();

        if (group.applyMembershipChange(removedMember, addedMember, expectedMembersCommitIndex, newMembersCommitIndex)) {
            if (logger.isFineEnabled()) {
                logger.fine("Applied add-member: " + (addedMember != null ? addedMember : "-") + " and remove-member: "
                        + (removedMember != null ? removedMember : "-") + " in "  + group.id()
                        + " with new members commit index: " + newMembersCommitIndex);
            }
            if (getLocalCPMember().equals(addedMember)) {
                // we are the added member to the group, we can try to create the local raft node if not created already
                raftService.createRaftNode(group.id(), group.memberImpls());
            }

            return true;
        }

        logger.severe("Could not apply add-member: " + (addedMember != null ? addedMember : "-")
                + " and remove-member: " + (removedMember != null ? removedMember : "-") + " in "  + group
                + " with new members commit index: " + newMembersCommitIndex + " expected members commit index: "
                + expectedMembersCommitIndex + " known members commit index: " + group.getMembersCommitIndex());

        return false;
    }

    private boolean checkSafeToRemoveIfCPMemberLeaving(MembershipChangeSchedule schedule) {
        CPMemberInfo leavingMember = schedule.getLeavingMember();
        if (leavingMember == null) {
            return false;
        }

        if (schedule.getChanges().size() > 0) {
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

    private List<CPGroupMembershipChange> getGroupMembershipChangesForNewMember(CPMemberInfo newMember) {
        List<CPGroupMembershipChange> changes = new ArrayList<CPGroupMembershipChange>();
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == ACTIVE && group.initialMemberCount() > group.memberCount()) {
                checkState(!group.memberImpls().contains(newMember), group + " already contains: " + newMember);

                changes.add(new CPGroupMembershipChange(group.id(), group.getMembersCommitIndex(), group.memberImpls(),
                        newMember, null));
            }
        }

        return changes;
    }

    public Collection<CPMemberInfo> getActiveMembers() {
        return activeMembers;
    }

    public void handleMetadataGroupId(RaftGroupId newMetadataGroupId) {
        checkNotNull(newMetadataGroupId);
        RaftGroupId metadataGroupId = getMetadataGroupId();
        while (metadataGroupId.seed() < newMetadataGroupId.seed()) {
            if (metadataGroupIdRef.compareAndSet(metadataGroupId, newMetadataGroupId)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Updated METADATA groupId: " + newMetadataGroupId);
                }

                return;
            }

            metadataGroupId = getMetadataGroupId();
        }
    }

    private void updateInvocationManagerMembers(long groupIdSeed, long membersCommitIndex, Collection<CPMemberInfo> members) {
        RaftInvocationContext context = raftService.getInvocationManager().getRaftInvocationContext();
        context.setMembers(groupIdSeed, membersCommitIndex, members);
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

    public MembershipChangeSchedule getMembershipChangeSchedule() {
        return membershipChangeSchedule;
    }

    // could return stale information
    boolean isMetadataGroupLeader() {
        CPMemberInfo localCPMember = getLocalCPMember();
        if (localCPMember == null) {
            return false;
        }
        RaftNode raftNode = raftService.getRaftNode(getMetadataGroupId());
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown() && localCPMember.equals(raftNode.getLeader());
    }

    /**
     * this method is idempotent
     */
    public boolean addMember(long commitIndex, CPMemberInfo member) {
        checkNotNull(member);
        checkMetadataGroupInitSuccessful();

        for (CPMemberInfo existingMember : activeMembers) {
            if (existingMember.getAddress().equals(member.getAddress())) {
                if (existingMember.getUuid().equals(member.getUuid())) {
                    if (logger.isFineEnabled()) {
                        logger.fine(member + " already exists.");
                    }

                    if (membershipChangeSchedule != null && member.equals(membershipChangeSchedule.getAddedMember())) {
                        membershipChangeSchedule = membershipChangeSchedule.addRetriedCommitIndex(commitIndex);
                        logger.info("CP groups are already being rebalanced for " + member);
                        return false;
                    }

                    return true;
                }

                throw new IllegalStateException(member + " cannot be added to the CP subsystem because another " + existingMember
                        + " exists with the same address!");
            }
        }

        checkState(membershipChangeSchedule == null,
                "Cannot rebalance CP groups because there is ongoing " + membershipChangeSchedule);

        Collection<CPMemberInfo> newMembers = new LinkedHashSet<CPMemberInfo>(activeMembers);
        newMembers.add(member);
        doSetActiveMembers(commitIndex, newMembers);
        logger.info("Added new " + member + ". New active CP members list: " + newMembers);

        List<CPGroupMembershipChange> changes = getGroupMembershipChangesForNewMember(member);
        if (changes.size() > 0) {
            membershipChangeSchedule = MembershipChangeSchedule.forJoiningMember(singletonList(commitIndex), member, changes);
            if (logger.isFineEnabled()) {
                logger.fine("CP group rebalancing is triggered for " + member + ", changes: " + membershipChangeSchedule);
            }

            return false;
        }

        return true;
    }

    private void removeActiveMember(long commitIndex, CPMemberInfo member) {
        Collection<CPMemberInfo> newMembers = new LinkedHashSet<CPMemberInfo>(activeMembers);
        newMembers.remove(member);
        doSetActiveMembers(commitIndex, newMembers);
    }

    private void doSetActiveMembers(long commitIndex, Collection<CPMemberInfo> members) {
        // first set the active members, then set the commit index.
        // because readers will use commit index for comparison, etc.
        // When a caller reads commit index first, it knows that the active members
        // it has read is at least up to date as the commit index
        activeMembers = unmodifiableCollection(members);
        activeMembersCommitIndex = commitIndex;
        updateInvocationManagerMembers(getMetadataGroupId().seed(), commitIndex, activeMembers);
        raftService.updateMissingMembers();
        broadcastActiveCPMembers();
    }

    public void checkMetadataGroupInitSuccessful() {
        switch (initializationStatus) {
            case SUCCESSFUL:
                return;
            case IN_PROGRESS:
                throw new MetadataRaftGroupInitInProgressException();
            case FAILED:
                throw new IllegalStateException("CP subsystem initialization failed!");
            default:
                throw new IllegalStateException("Illegal initialization status: " + initializationStatus);

        }
    }

    void broadcastActiveCPMembers() {
        if (!(isDiscoveryCompleted() && isMetadataGroupLeader())) {
            return;
        }

        RaftGroupId metadataGroupId = getMetadataGroupId();
        long commitIndex = this.activeMembersCommitIndex;
        Collection<CPMemberInfo> cpMembers = this.activeMembers;

        if (cpMembers.isEmpty()) {
            return;
        }

        Set<Member> clusterMembers = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new PublishActiveCPMembersOp(metadataGroupId, commitIndex, cpMembers);
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

    List<CPMemberInfo> getInitialCPMembers() {
        return initialCPMembers;
    }

    MetadataRaftGroupInitStatus getInitializationStatus() {
        return initializationStatus;
    }

    Set<CPMemberInfo> getInitializedCPMembers() {
        return initializedCPMembers;
    }

    Set<Long> getInitializationCommitIndices() {
        return initializationCommitIndices;
    }

    public void disableDiscovery() {
        if (config.getCPMemberCount() > 0) {
            logger.info("Disabling discovery of initial CP members since it is already completed...");
        }

        discoveryCompleted.set(true);
    }

    private void scheduleDiscoverInitialCPMembersTask(boolean terminateOnDiscoveryFailure) {
        Runnable task = new DiscoverInitialCPMembersTask(terminateOnDiscoveryFailure);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(task, DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    private class BroadcastActiveCPMembersTask implements Runnable {
        @Override
        public void run() {
            broadcastActiveCPMembers();
        }
    }

    private class DiscoverInitialCPMembersTask implements Runnable {

        private Collection<Member> latestMembers = Collections.emptySet();
        private final boolean terminateOnDiscoveryFailure;
        private long lastLoggingTime;

        DiscoverInitialCPMembersTask(boolean terminateOnDiscoveryFailure) {
            this.terminateOnDiscoveryFailure = terminateOnDiscoveryFailure;
        }

        @Override
        public void run() {
            if (shouldRescheduleOrSkip()) {
                return;
            }

            Collection<Member> members = nodeEngine.getClusterService().getMembers();
            for (Member member : latestMembers) {
                if (!members.contains(member)) {
                    logger.severe(member + " left the cluster while CP subsystem discovery in progress!");
                    handleDiscoveryFailure();
                    return;
                }
            }

            latestMembers = members;

            if (rescheduleIfCPMemberCountNotSatisfied(members)) {
                return;
            }

            CPMemberInfo localMemberCandidate = new CPMemberInfo(nodeEngine.getLocalMember());
            List<CPMemberInfo> discoveredCPMembers = getDiscoveredCPMembers(members);

            if (completeDiscoveryIfNotCPMember(discoveredCPMembers, localMemberCandidate)) {
                return;
            }

            // we must update invocation manager's member list before making the first raft invocation
            updateInvocationManagerMembers(getMetadataGroupId().seed(), 0, discoveredCPMembers);

            if (!commitMetadataRaftGroupInit(localMemberCandidate, discoveredCPMembers)) {
                handleDiscoveryFailure();
                return;
            }

            logger.info("CP subsystem is initialized with: " + discoveredCPMembers);
            discoveryCompleted.set(true);
            broadcastActiveCPMembers();
            scheduleRaftGroupMembershipManagementTasks();
        }

        private boolean shouldRescheduleOrSkip() {
            // When a node joins to the cluster, first, discoveryCompleted flag is set, then the join flag is set.
            // Hence, we need to check these flags in the reverse order here.

            if (!nodeEngine.getClusterService().isJoined()) {
                scheduleSelf();
                return true;
            }

            // RU_COMPAT_3_11
            if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_12)) {
                logger.fine("Cannot start initial CP members discovery since cluster version is less than 3.12.");
                scheduleSelf();
                return true;
            }

            return isDiscoveryCompleted();
        }

        private boolean rescheduleIfCPMemberCountNotSatisfied(Collection<Member> members) {
            if (members.size() < config.getCPMemberCount()) {
                long now = Clock.currentTimeMillis();
                if (now - lastLoggingTime >= DISCOVER_INITIAL_CP_MEMBERS_TASK_LOGGING_DELAY_MILLIS) {
                    lastLoggingTime = now;
                    logger.info("CP Subsystem is waiting for " + config.getCPMemberCount() + " members to join the cluster. "
                            + "Current member count: " + members.size());
                }

                scheduleSelf();
                return true;
            }
            return false;
        }

        private void scheduleSelf() {
            nodeEngine.getExecutionService()
                      .schedule(this, DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
        }

        private List<CPMemberInfo> getDiscoveredCPMembers(Collection<Member> members) {
            assert members.size() >= config.getCPMemberCount();
            List<Member> memberList = new ArrayList<Member>(members).subList(0, config.getCPMemberCount());
            List<CPMemberInfo> cpMembers = new ArrayList<CPMemberInfo>(config.getCPMemberCount());
            for (Member member : memberList) {
                // During the discovery process (both initial or cp subsystem restart),
                // it's guaranteed that AP and CP member UUIDs will be the same.
                cpMembers.add(new CPMemberInfo(member));
            }

            sort(cpMembers, new CPMemberComparator());
            return cpMembers;
        }

        private boolean completeDiscoveryIfNotCPMember(List<CPMemberInfo> cpMembers, CPMemberInfo localCPMemberCandidate) {
            if (!cpMembers.contains(localCPMemberCandidate)) {
                logger.info("I am not a CP member! I'll serve as an AP member.");
                discoveryCompleted.set(true);
                return true;
            }

            return false;
        }

        private boolean commitMetadataRaftGroupInit(CPMemberInfo localCPMemberCandidate, List<CPMemberInfo> discoveredCPMembers) {
            List<CPMemberInfo> metadataMembers = discoveredCPMembers.subList(0, config.getGroupSize());
            RaftGroupId metadataGroupId = getMetadataGroupId();
            try {
                if (metadataMembers.contains(localCPMemberCandidate)) {
                    raftService.createRaftNode(metadataGroupId, metadataMembers, localCPMemberCandidate);
                }

                RaftOp op = new InitMetadataRaftGroupOp(localCPMemberCandidate, discoveredCPMembers, metadataGroupId.seed());
                raftService.getInvocationManager().invoke(metadataGroupId, op).get();
                // By default, we use the same member UUID for both AP and CP members.
                // But it's not guaranteed to be same. For example;
                // - During a split-brain merge, AP member UUID is renewed but CP member UUID remains the same.
                // - While promoting a member to CP when Hot Restart is enabled, CP member doesn't use the AP member's UUID
                // but instead generates a new UUID.
                localCPMember.set(localCPMemberCandidate);
            } catch (Exception e) {
                logger.severe("Could not initialize METADATA CP group with CP members: " + metadataMembers, e);
                raftService.destroyRaftNode(metadataGroupId);
                return false;
            }
            return true;
        }

        private void handleDiscoveryFailure() {
            if (terminateOnDiscoveryFailure) {
                logger.warning("Terminating because of CP discovery failure...");
                terminateNode();
            } else {
                logger.warning("Cancelling CP subsystem discovery...");
                discoveryCompleted.set(true);
            }
        }

        private void terminateNode() {
            ((NodeEngineImpl) nodeEngine).getNode().shutdown(true);
        }
    }

    @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
    private static class CPMemberComparator implements Comparator<CPMemberInfo> {
        @Override
        public int compare(CPMemberInfo o1, CPMemberInfo o2) {
            return o1.getUuid().compareTo(o2.getUuid());
        }
    }

    @SuppressFBWarnings({"SE_COMPARATOR_SHOULD_BE_SERIALIZABLE", "DM_BOXED_PRIMITIVE_FOR_COMPARE"})
    private static class CPGroupIdComparator implements Comparator<CPGroupId> {
        @Override
        public int compare(CPGroupId o1, CPGroupId o2) {
            return Long.valueOf(o1.id()).compareTo(o2.id());
        }
    }
}
