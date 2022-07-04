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

import com.hazelcast.cluster.Member;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.cp.event.CPMembershipEvent.EventType;
import com.hazelcast.cp.event.impl.CPMembershipEventImpl;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.exception.MetadataRaftGroupInitInProgressException;
import com.hazelcast.cp.internal.persistence.CPMetadataStore;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raftop.metadata.InitMetadataRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.PublishActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.TerminateRaftNodesOp;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.ACTIVE;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYED;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYING;
import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.MembershipChangeSchedule.CPGroupMembershipChange;
import static com.hazelcast.cp.internal.RaftService.CP_SUBSYSTEM_EXECUTOR;
import static com.hazelcast.cp.internal.RaftService.CP_SUBSYSTEM_MANAGEMENT_EXECUTOR;
import static com.hazelcast.cp.internal.RaftService.EVENT_TOPIC_MEMBERSHIP;
import static com.hazelcast.cp.internal.RaftService.SERVICE_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_METADATA_RAFT_GROUP_MANAGER_ACTIVE_MEMBERS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_METADATA_RAFT_GROUP_MANAGER_ACTIVE_MEMBERS_COMMIT_INDEX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_METADATA_RAFT_GROUP_MANAGER_GROUPS;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Maintains CP Subsystem metadata, such as CP groups, active CP members,
 * leaving and joining CP members, etc.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
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

    private final NodeEngineImpl nodeEngine;
    private final RaftService raftService;
    private final RaftGroupMembershipManager membershipManager;
    private final ILogger logger;
    private final CPSubsystemConfig config;

    // these fields are related to the local CP member but they are not maintained within the Metadata CP group
    private final AtomicReference<CPMemberInfo> localCPMember = new AtomicReference<>();
    private final AtomicReference<RaftGroupId> metadataGroupIdRef = new AtomicReference<>(INITIAL_METADATA_GROUP_ID);
    private final AtomicBoolean discoveryCompleted = new AtomicBoolean();
    private final boolean cpSubsystemEnabled;
    private volatile DiscoverInitialCPMembersTask currentDiscoveryTask;

    // all fields below are state of the Metadata CP group and put into Metadata snapshot and reset while restarting...
    // these fields are accessed outside of Raft while restarting or local querying, etc.
    @Probe(name = CP_METRIC_METADATA_RAFT_GROUP_MANAGER_GROUPS)
    private final ConcurrentMap<CPGroupId, CPGroupInfo> groups = new ConcurrentHashMap<>();
    // activeMembers must be an ordered non-null collection
    @Probe(name = CP_METRIC_METADATA_RAFT_GROUP_MANAGER_ACTIVE_MEMBERS)
    private volatile Collection<CPMemberInfo> activeMembers = Collections.emptySet();
    @Probe(name = CP_METRIC_METADATA_RAFT_GROUP_MANAGER_ACTIVE_MEMBERS_COMMIT_INDEX)
    private volatile long activeMembersCommitIndex;
    private volatile List<CPMemberInfo> initialCPMembers;
    private volatile MembershipChangeSchedule membershipChangeSchedule;
    private volatile MetadataRaftGroupInitStatus initializationStatus = MetadataRaftGroupInitStatus.IN_PROGRESS;
    private final Set<CPMemberInfo> initializedCPMembers = newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Long> initializationCommitIndices = newSetFromMap(new ConcurrentHashMap<>());

    MetadataRaftGroupManager(NodeEngineImpl nodeEngine, RaftService raftService, CPSubsystemConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.membershipManager = new RaftGroupMembershipManager(nodeEngine, raftService);
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;
        this.cpSubsystemEnabled = raftService.isCpSubsystemEnabled();
    }

    boolean init() {
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
        try {
            getCpMetadataStore().persistLocalCPMember(member);
        } catch (IOException e) {
            throw new HazelcastException(e);
        }
        scheduleGroupMembershipManagementTasks();
    }

    private void scheduleGroupMembershipManagementTasks() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(CP_SUBSYSTEM_MANAGEMENT_EXECUTOR, new BroadcastActiveCPMembersTask(), 0,
                BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS, SECONDS);

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

        localCPMember.set(null);

        DiscoverInitialCPMembersTask discoveryTask = currentDiscoveryTask;
        if (discoveryTask != null) {
            discoveryTask.cancelAndAwaitCompletion();
        }
        discoveryCompleted.set(false);

        RaftGroupId newMetadataGroupId = new RaftGroupId(METADATA_CP_GROUP_NAME, seed, 0);
        logger.fine("New METADATA groupId: " + newMetadataGroupId);
        metadataGroupIdRef.set(newMetadataGroupId);
        try {
            getCpMetadataStore().persistMetadataGroupId(newMetadataGroupId);
        } catch (IOException e) {
            throw new HazelcastException(e);
        }

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

        Set<CPGroupId> snapshotGroupIds = new HashSet<>();
        for (CPGroupInfo group : snapshot.getGroups()) {
            groups.put(group.id(), group);
            snapshotGroupIds.add(group.id());
        }

        groups.keySet().removeIf(cpGroupId -> !snapshotGroupIds.contains(cpGroupId));

        doSetActiveMembers(snapshot.getMembersCommitIndex(), new LinkedHashSet<>(snapshot.getMembers()));
        membershipChangeSchedule = snapshot.getMembershipChangeSchedule();
        initialCPMembers = snapshot.getInitialCPMembers();
        initializedCPMembers.clear();
        initializedCPMembers.addAll(snapshot.getInitializedCPMembers());
        initializationStatus = snapshot.getInitializationStatus();
        initializationCommitIndices.clear();
        initializationCommitIndices.addAll(snapshot.getInitializationCommitIndices());

        for (CPGroupInfo group : snapshot.getGroups()) {
            if (group.status() == DESTROYED) {
                terminateRaftNodeAsync(group.id());
            }
        }

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

    public void restoreMetadataGroupId(RaftGroupId restoredMetadataGroupId) {
        if (raftService.isStartCompleted()) {
            throw new IllegalStateException("Cannot set metadata groupId after start process is completed!");
        }

        RaftGroupId currentMetadataGroupId = getMetadataGroupId();
        if (restoredMetadataGroupId.getSeed() <= currentMetadataGroupId.getSeed()) {
            // I might be already received a newer METADATA group id even before I restore mine
            logger.fine("Not restoring METADATA groupId: " + restoredMetadataGroupId + " because the current METADATA groupId: "
                    + currentMetadataGroupId + " is newer.");
            return;
        }

        if (currentMetadataGroupId.getSeed() != INITIAL_METADATA_GROUP_ID.getSeed()
            || initializationStatus != MetadataRaftGroupInitStatus.IN_PROGRESS
            || !initializedCPMembers.isEmpty()
            || !groups.isEmpty()) {
            throw new IllegalStateException("Metadata groupId is not allowed to be set!");
        }

        metadataGroupIdRef.set(restoredMetadataGroupId);

        logger.fine("Restored METADATA groupId: " + restoredMetadataGroupId);
    }

    public void restoreLocalCPMember(CPMemberInfo member) {
        checkNotNull(member);
        if (raftService.isStartCompleted()) {
            throw new IllegalStateException("Cannot set local CP member after start process is completed!");
        }
        if (!localCPMember.compareAndSet(null, member)) {
            throw new IllegalStateException("Local CP member is already set! Current: " + localCPMember.get());
        }

        scheduleGroupMembershipManagementTasks();
    }

    long getGroupIdSeed() {
        return getMetadataGroupId().getSeed();
    }

    public Collection<CPGroupId> getGroupIds() {
        List<CPGroupId> groupIds = new ArrayList<>(groups.keySet());
        groupIds.sort(new CPGroupIdComparator());

        return groupIds;
    }

    public Collection<CPGroupId> getActiveGroupIds() {
        List<CPGroupId> activeGroupIds = new ArrayList<>(1);
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == ACTIVE) {
                activeGroupIds.add(group.id());
            }
        }

        activeGroupIds.sort(new CPGroupIdComparator());

        return activeGroupIds;
    }

    public CPGroupSummary getGroup(CPGroupId groupId) {
        checkNotNull(groupId);

        if ((groupId instanceof RaftGroupId) && ((RaftGroupId) groupId).getSeed() < getGroupIdSeed()) {
            throw new CPGroupDestroyedException(groupId);
        }

        CPGroupInfo group = groups.get(groupId);
        return group != null ? group.toSummary(activeMembers) : null;
    }

    public CPGroupSummary getActiveGroup(String groupName) {
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == CPGroupStatus.ACTIVE && group.name().equals(groupName)) {
                return group.toSummary(activeMembers);
            }
        }

        return null;
    }

    public void rebalanceGroupLeaderships() {
        if (!isMetadataGroupLeader()) {
            return;
        }
        membershipManager.rebalanceGroupLeaderships();
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    public boolean initMetadataGroup(long commitIndex, CPMemberInfo callerCPMember, List<CPMemberInfo> discoveredCPMembers,
                                     long expectedGroupIdSeed) {
        checkNotNull(discoveredCPMembers);

        // Fail fast if CP Subsystem initialization is already failed
        if (initializationStatus == MetadataRaftGroupInitStatus.FAILED) {
            String msg = callerCPMember + "committed CP member list: " + discoveredCPMembers
                    + " after CP Subsystem discovery has already failed.";
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

        List<RaftEndpoint> discoveredMetadataEndpoints = new ArrayList<>();
        for (CPMemberInfo member : discoveredCPMembers) {
            if (discoveredMetadataEndpoints.size() == config.getGroupSize()) {
                break;
            }
            discoveredMetadataEndpoints.add(member.toRaftEndpoint());
        }

        CPGroupInfo metadataGroup = new CPGroupInfo(getMetadataGroupId(), discoveredMetadataEndpoints);
        CPGroupInfo existingMetadataGroup = groups.putIfAbsent(getMetadataGroupId(), metadataGroup);
        if (existingMetadataGroup != null) {
            Collection<RaftEndpoint> metadataEndpoints = existingMetadataGroup.initialMembers();
            if (discoveredMetadataEndpoints.size() != metadataEndpoints.size()
                    || !metadataEndpoints.containsAll(discoveredMetadataEndpoints)) {
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

        initialCPMembers = unmodifiableList(new ArrayList<>(discoveredCPMembers));
        doSetActiveMembers(commitIndex, new LinkedHashSet<>(discoveredCPMembers));

        if (initializedCPMembers.size() == config.getCPMemberCount()) {
            // All CP members have committed their initialization

            // remove this commit because we will return the response directly here
            initializationCommitIndices.remove(commitIndex);

            logger.fine("METADATA " + metadataGroup + " initialization is completed with: " + initializedCPMembers);

            initializationStatus = MetadataRaftGroupInitStatus.SUCCESSFUL;
            Collection<Long> completed = new ArrayList<>(initializationCommitIndices);
            initializedCPMembers.clear();
            initializationCommitIndices.clear();
            raftService.updateInvocationManagerMembers(groupIdSeed, commitIndex, activeMembers);
            completeFutures(getMetadataGroupId(), completed, null);

            return true;
        }

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

    public CPGroupSummary createRaftGroup(String groupName, Collection<RaftEndpoint> groupEndpoints, long groupId) {
        checkFalse(equalsIgnoreCase(METADATA_CP_GROUP_NAME, groupName), groupName + " is reserved for internal usage!");
        checkMetadataGroupInitSuccessful();

        // keep configuration on every metadata node
        CPGroupInfo group = getRaftGroupByName(groupName);
        if (group != null) {
            if (group.memberCount() == groupEndpoints.size()) {
                if (logger.isFineEnabled()) {
                    logger.fine("CP group " + groupName + " already exists.");
                }

                return group.toSummary(activeMembers);
            }

            String msg = group.id() + " already exists with a different size: " + group.memberCount();
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        group = getRaftGroupById(groupId);
        if (group != null) {
            throw new CannotCreateRaftGroupException("Cannot create CP group: " + groupName + " with members: " + groupEndpoints
                    + " because group index: " + groupId + " already belongs to " + group.name());
        }

        Map<UUID, CPMemberInfo> activeMembersMap = getActiveMembersMap();

        CPMemberInfo leavingMember = membershipChangeSchedule != null ? membershipChangeSchedule.getLeavingMember() : null;
        for (RaftEndpoint groupEndpoint : groupEndpoints) {
            if ((leavingMember != null && groupEndpoint.getUuid().equals(leavingMember.getUuid()))
                    || !activeMembersMap.containsKey(groupEndpoint.getUuid())) {
                String msg = "Cannot create CP group: " + groupName + " since " + groupEndpoint + " is not active";
                if (logger.isFineEnabled()) {
                    logger.fine(msg);
                }

                throw new CannotCreateRaftGroupException(msg);
            }
        }

        return createRaftGroup(new CPGroupInfo(new RaftGroupId(groupName, getGroupIdSeed(), groupId), groupEndpoints));
    }

    private CPGroupSummary createRaftGroup(CPGroupInfo group) {
        addRaftGroup(group);

        Map<UUID, CPMemberInfo> activeMembersMap = getActiveMembersMap();

        List<CPMemberInfo> members = new ArrayList<>();
        for (RaftEndpoint member : group.members()) {
            members.add(activeMembersMap.get(member.getUuid()));
        }
        logger.info("New " + group.id() + " is created with " + members);

        return group.toSummary(activeMembers);
    }

    private void createRaftNodeAsync(CPGroupInfo group) {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.execute(CP_SUBSYSTEM_EXECUTOR, () -> raftService.createRaftNode(group.id(), group.members()));
    }

    private Map<UUID, CPMemberInfo> getActiveMembersMap() {
        Map<UUID, CPMemberInfo> map = new HashMap<>();
        for (CPMemberInfo member : activeMembers) {
            map.put(member.getUuid(), member);
        }
        return map;
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

    private CPGroupInfo getRaftGroupById(long groupId) {
        for (CPGroupInfo group : groups.values()) {
            if (group.id().getId() == groupId) {
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
            sendTerminateRaftNodeOpsForDestroyedGroup(group);
        } else if (logger.isFineEnabled()) {
            logger.fine(groupId + " is already destroyed.");
        }
    }

    public void forceDestroyRaftGroup(String groupName) {
        checkNotNull(groupName);
        checkFalse(equalsIgnoreCase(METADATA_CP_GROUP_NAME, groupName), "Cannot force-destroy the METADATA CP group!");
        checkMetadataGroupInitSuccessful();

        boolean found = false;

        for (CPGroupInfo group : groups.values()) {
            if (group.name().equals(groupName)) {
                if (group.forceSetDestroyed()) {
                    logger.info(group.id() + " is force-destroyed.");
                    sendTerminateRaftNodeOpsForDestroyedGroup(group);
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

    private void sendTerminateRaftNodeOpsForDestroyedGroup(CPGroupInfo group) {
        Map<UUID, CPMemberInfo> activeMembersMap = getActiveMembersMap();
        CPMemberInfo localCPMember = getLocalCPMember();
        if (localCPMember == null) {
            return;
        }
        RaftEndpoint localEndpoint = localCPMember.toRaftEndpoint();
        OperationService operationService = nodeEngine.getOperationService();
        for (RaftEndpoint endpoint : group.members()) {
            if (endpoint.equals(localEndpoint)) {
                terminateRaftNodeAsync(group.id());
            } else {
                Operation op = new TerminateRaftNodesOp(Collections.singleton(group.id()));
                CPMemberInfo cpMember = activeMembersMap.get(endpoint.getUuid());
                operationService.invokeOnTarget(SERVICE_NAME, op, cpMember.getAddress());
            }
        }
    }

    private void terminateRaftNodeAsync(CPGroupId groupId) {
        nodeEngine.getExecutionService().execute(CP_SUBSYSTEM_EXECUTOR, () -> raftService.terminateRaftNode(groupId, true));
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
        List<CPGroupId> leavingGroupIds = new ArrayList<>();
        List<CPGroupMembershipChange> changes = new ArrayList<>();
        for (CPGroupInfo group : groups.values()) {
            CPGroupId groupId = group.id();
            if (!group.containsMember(leavingMember.toRaftEndpoint()) || group.status() == DESTROYED) {
                continue;
            }

            CPMemberInfo substitute = findSubstitute(group);
            RaftEndpoint substituteEndpoint = substitute != null ? substitute.toRaftEndpoint() : null;
            leavingGroupIds.add(groupId);
            changes.add(new CPGroupMembershipChange(groupId, group.getMembersCommitIndex(), group.memberImpls(),
                    substituteEndpoint, leavingMember.toRaftEndpoint()));
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
            if (activeMembers.contains(substitute) && !group.containsMember(substitute.toRaftEndpoint())) {
                return substitute;
            }
        }

        return null;
    }

    public MembershipChangeSchedule completeRaftGroupMembershipChanges(long commitIndex,
                                                                       Map<CPGroupId, BiTuple<Long, Long>> changedGroups) {
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
            BiTuple<Long, Long> t = changedGroups.get(groupId);

            if (t != null) {
                if (!applyMembershipChange(change, group, t.element1, t.element2)) {
                    changedGroups.remove(groupId);
                }
            } else if (group.status() == DESTROYED && !changedGroups.containsKey(groupId)) {
                if (logger.isFineEnabled()) {
                    logger.warning(groupId + " is already destroyed so will skip: " + change);
                }
                changedGroups.put(groupId, BiTuple.of(0L, 0L));
            }
        }

        membershipChangeSchedule = membershipChangeSchedule.excludeCompletedChanges(changedGroups.keySet());

        if (checkSafeToRemoveIfCPMemberLeaving(membershipChangeSchedule)) {
            CPMemberInfo leavingMember = membershipChangeSchedule.getLeavingMember();
            removeActiveMember(commitIndex, leavingMember);
            completeFutures(getMetadataGroupId(), membershipChangeSchedule.getMembershipChangeCommitIndices(), null);
            membershipChangeSchedule = null;
            logger.info(leavingMember + " is removed from CP Subsystem.");

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
        RaftEndpoint addedMember = change.getMemberToAdd();
        RaftEndpoint removedMember = change.getMemberToRemove();

        if (group.applyMembershipChange(removedMember, addedMember, expectedMembersCommitIndex, newMembersCommitIndex)) {
            if (logger.isFineEnabled()) {
                logger.fine("Applied add-member: " + (addedMember != null ? addedMember : "-") + " and remove-member: "
                        + (removedMember != null ? removedMember : "-") + " in "  + group.id()
                        + " with new members commit index: " + newMembersCommitIndex);
            }

            return true;
        }

        logger.severe("Could not apply add-member: " + (addedMember != null ? addedMember : "-")
                + " and remove-member: " + (removedMember != null ? removedMember : "-") + " in "  + group
                + " with new members commit index: " + newMembersCommitIndex + ", expected members commit index: "
                + expectedMembersCommitIndex + ", known members commit index: " + group.getMembersCommitIndex());

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

        RaftEndpoint leavingEndpoint = leavingMember.toRaftEndpoint();
        for (CPGroupInfo group : groups.values()) {
            if (group.containsMember(leavingEndpoint)) {
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
        List<CPGroupMembershipChange> changes = new ArrayList<>();
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == ACTIVE && group.initialMemberCount() > group.memberCount()) {
                checkState(!group.memberImpls().contains(newMember.toRaftEndpoint()), group + " already contains: " + newMember);

                changes.add(new CPGroupMembershipChange(group.id(), group.getMembersCommitIndex(), group.memberImpls(),
                        newMember.toRaftEndpoint(), null));
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

        // During pre-join, CP data restore process won't be started yet.
        // If persistence enabled and this member has CP data persisted
        // then it has to wait until CP restore process completes
        // before processing metadataGroupId update.
        // So, during pre-join we skip the update.
        // If this member does not have any persisted CP data,
        // then it's ok to process the update even though persistence is enabled.
        CPMetadataStore metadataStore = getCpMetadataStore();
        if (!raftService.isStartCompleted() && metadataStore.containsLocalMemberFile()) {
            if (!metadataGroupId.equals(newMetadataGroupId)) {
                logger.severe("Restored METADATA groupId: " + metadataGroupId + " is different than received METADATA groupId: "
                        + newMetadataGroupId + ". There must have been a CP Subsystem reset while this member was down...");
            }

            return;
        }

        if (metadataGroupId.getSeed() >= newMetadataGroupId.getSeed()) {
            return;
        }

        metadataGroupId = getMetadataGroupId();
        if (metadataGroupId.getSeed() >= newMetadataGroupId.getSeed()) {
            return;
        }
        metadataGroupIdRef.set(newMetadataGroupId);
        try {
            metadataStore.persistMetadataGroupId(newMetadataGroupId);
        } catch (IOException e) {
            throw new HazelcastException(e);
        }
    }

    public Collection<CPGroupId> getDestroyingGroupIds() {
        Collection<CPGroupId> groupIds = new ArrayList<>();
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
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown()
                && localCPMember.toRaftEndpoint().equals(raftNode.getLeader());
    }

    public void verifyRestartedMember(long commitIndex, CPMemberInfo member) {
        checkNotNull(member);
        checkMetadataGroupInitSuccessful();

        LinkedHashSet<CPMemberInfo> newMembers = new LinkedHashSet<>(activeMembers.size());
        boolean found = false;

        for (CPMemberInfo existingMember : activeMembers) {
            if (existingMember.getUuid().equals(member.getUuid())) {
                if (existingMember.getAddress().equals(member.getAddress())) {
                    logger.fine(member + " already exists.");
                    return;
                }
                logger.info("Replaced " + existingMember + " with " + member);
                newMembers.add(member);
                found = true;
            } else {
                newMembers.add(existingMember);
            }
        }

        if (!found) {
            throw new IllegalStateException(member + " does not exist in the active CP members list!");
        }

        logger.info("New active CP members list: " + newMembers);
        doSetActiveMembers(commitIndex, newMembers);
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

                throw new IllegalStateException(member + " cannot be added to CP Subsystem because another " + existingMember
                        + " exists with the same address!");
            }
        }

        checkState(membershipChangeSchedule == null,
                "Cannot rebalance CP groups because there is ongoing " + membershipChangeSchedule);

        LinkedHashSet<CPMemberInfo> newMembers = new LinkedHashSet<>(activeMembers);
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
        LinkedHashSet<CPMemberInfo> newMembers = new LinkedHashSet<>(activeMembers);
        newMembers.remove(member);
        doSetActiveMembers(commitIndex, newMembers);
    }

    @SuppressWarnings("checkstyle:illegaltype")
    private void doSetActiveMembers(long commitIndex, LinkedHashSet<CPMemberInfo> members) {
        // first set the active members, then set the commit index.
        // because readers will use commit index for comparison, etc.
        // When a caller reads commit index first, it knows that the active members
        // it has read is at least up to date as the commit index

        Collection<CPMemberInfo> currentMembers = activeMembers;
        activeMembers = unmodifiableCollection(members);
        activeMembersCommitIndex = commitIndex;
        try {
            logger.fine("Persisting active CP members " + activeMembers + " with commitIndex " + activeMembersCommitIndex);
            getCpMetadataStore().persistActiveCPMembers(activeMembers, activeMembersCommitIndex);
        } catch (IOException e) {
            throw new HazelcastException(e);
        }

        raftService.updateInvocationManagerMembers(getMetadataGroupId().getSeed(), commitIndex, activeMembers);
        raftService.updateMissingMembers();
        broadcastActiveCPMembers();
        sendMembershipEvents(currentMembers, members);
    }

    private void sendMembershipEvents(Collection<CPMemberInfo> currentMembers, Collection<CPMemberInfo> newMembers) {
        if (!isMetadataGroupLeader()) {
             return;
        }

        EventService eventService = nodeEngine.getEventService();

        Collection<CPMemberInfo> addedMembers = new LinkedHashSet<>(newMembers);
        addedMembers.removeAll(currentMembers);

        for (CPMemberInfo member : addedMembers) {
            CPMembershipEvent event = new CPMembershipEventImpl(member, EventType.ADDED);
            eventService.publishEvent(SERVICE_NAME, EVENT_TOPIC_MEMBERSHIP, event, EVENT_TOPIC_MEMBERSHIP.hashCode());
        }

        Collection<CPMemberInfo> removedMembers = new LinkedHashSet<>(currentMembers);
        removedMembers.removeAll(newMembers);

        for (CPMemberInfo member : removedMembers) {
            CPMembershipEvent event = new CPMembershipEventImpl(member, EventType.REMOVED);
            eventService.publishEvent(SERVICE_NAME, EVENT_TOPIC_MEMBERSHIP, event, EVENT_TOPIC_MEMBERSHIP.hashCode());
        }
    }

    public void checkMetadataGroupInitSuccessful() {
        switch (initializationStatus) {
            case SUCCESSFUL:
                return;
            case IN_PROGRESS:
                throw new MetadataRaftGroupInitInProgressException();
            case FAILED:
                throw new IllegalStateException("CP Subsystem initialization failed!");
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
        if (cpSubsystemEnabled) {
            logger.info("Disabling discovery of initial CP members since it is already completed...");
        }

        discoveryCompleted.set(true);
        try {
            getCpMetadataStore().tryMarkAPMember();
        } catch (IOException e) {
            throw new HazelcastException(e);
        }
    }

    private void scheduleDiscoverInitialCPMembersTask(boolean terminateOnDiscoveryFailure) {
        DiscoverInitialCPMembersTask task = new DiscoverInitialCPMembersTask(terminateOnDiscoveryFailure);
        currentDiscoveryTask = task;
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(CP_SUBSYSTEM_MANAGEMENT_EXECUTOR, task,
                DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    private class BroadcastActiveCPMembersTask implements Runnable {
        @Override
        public void run() {
            broadcastActiveCPMembers();
        }
    }

    private enum DiscoveryTaskState {
        RUNNING, SCHEDULED, COMPLETED
    }

    private class DiscoverInitialCPMembersTask implements Runnable {

        private Collection<Member> latestMembers = Collections.emptySet();
        private final boolean terminateOnDiscoveryFailure;
        private long lastLoggingTime;
        private volatile boolean cancelled;
        private volatile DiscoveryTaskState state;

        DiscoverInitialCPMembersTask(boolean terminateOnDiscoveryFailure) {
            this.terminateOnDiscoveryFailure = terminateOnDiscoveryFailure;
            state = DiscoveryTaskState.SCHEDULED;
        }

        @Override
        public void run() {
            state = DiscoveryTaskState.RUNNING;
            try {
                doRun();
            } finally {
                if (state == DiscoveryTaskState.RUNNING) {
                    state = DiscoveryTaskState.COMPLETED;
                }
            }
        }

        @SuppressWarnings("checkstyle:npathcomplexity")
        private void doRun() {
            if (shouldRescheduleOrSkip()) {
                return;
            }

            // runs after CP restore procedure is completed...

            boolean markedAPMember = getCpMetadataStore().isMarkedAPMember();

            if (!markedAPMember && localCPMember.get() == null) {
                logger.fine("Starting CP discovery...");

                // If there is no AP and CP identity restored,
                // it means that this member is starting from scratch
                // so we should run the CP discovery process...
                Collection<Member> members = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
                for (Member member : latestMembers) {
                    if (!members.contains(member)) {
                        logger.severe(member + " left the cluster while the CP discovery in progress!");
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
                raftService.updateInvocationManagerMembers(getMetadataGroupId().getSeed(), 0, discoveredCPMembers);

                if (!commitMetadataRaftGroupInit(localMemberCandidate, discoveredCPMembers)) {
                    handleDiscoveryFailure();
                    return;
                }
                logger.info("CP Subsystem is initialized with: " + discoveredCPMembers);
            }

            discoveryCompleted.set(true);

            if (localCPMember.get() != null) {
                scheduleGroupMembershipManagementTasks();
            }
        }

        /**
         * Returns {@code true} if task is skipped or rescheduled
         * or {@code false} if task should execute now.
         */
        private boolean shouldRescheduleOrSkip() {
            if (cancelled) {
                return true;
            }

            // When a node joins to the cluster, first, discoveryCompleted flag is set, then the join flag is set.
            // Hence, we need to check these flags in the reverse order here.

            if (!nodeEngine.getClusterService().isJoined()) {
                scheduleSelf();
                return true;
            }

            if (!raftService.isStartCompleted()) {
                logger.fine("Re-scheduling, startup is not completed yet!");
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
            state = DiscoveryTaskState.SCHEDULED;
            ExecutionService executionService = nodeEngine.getExecutionService();
            executionService.schedule(CP_SUBSYSTEM_MANAGEMENT_EXECUTOR, this,
                    DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
        }

        private List<CPMemberInfo> getDiscoveredCPMembers(Collection<Member> members) {
            assert members.size() >= config.getCPMemberCount();
            List<Member> memberList = new ArrayList<>(members).subList(0, config.getCPMemberCount());
            List<CPMemberInfo> cpMembers = new ArrayList<>(config.getCPMemberCount());
            for (Member member : memberList) {
                // During the discovery process (both initial or CP Subsystem restart),
                // it's guaranteed that AP and CP member UUIDs will be the same.
                cpMembers.add(new CPMemberInfo(member));
            }

            cpMembers.sort(new CPMemberComparator());
            return cpMembers;
        }

        private boolean completeDiscoveryIfNotCPMember(List<CPMemberInfo> cpMembers, CPMemberInfo localCPMemberCandidate) {
            if (!cpMembers.contains(localCPMemberCandidate)) {
                logger.info("I am not a CP member! I'll serve as an AP member.");
                try {
                    boolean marked = getCpMetadataStore().tryMarkAPMember();
                    assert marked;
                    discoveryCompleted.set(true);
                } catch (IOException e) {
                    throw new HazelcastException(e);
                }
                return true;
            }

            return false;
        }

        private boolean commitMetadataRaftGroupInit(CPMemberInfo localCPMemberCandidate, List<CPMemberInfo> discoveredCPMembers) {
            List<CPMemberInfo> metadataMembers = discoveredCPMembers.subList(0, config.getGroupSize());
            RaftGroupId metadataGroupId = getMetadataGroupId();
            try {
                // By default, we use the same member UUID for both AP and CP members.
                // But it's not guaranteed to be same. For example;
                // - During a split-brain merge, AP member UUID is renewed but CP member UUID remains the same.
                // - While promoting a member to CP when Hot Restart is enabled, CP member doesn't use the AP member's UUID
                // but instead generates a new UUID.
                localCPMember.set(localCPMemberCandidate);
                getCpMetadataStore().persistLocalCPMember(localCPMemberCandidate);

                if (metadataMembers.contains(localCPMemberCandidate)) {
                    List<RaftEndpoint> metadataEndpoints = new ArrayList<>();
                    for (CPMemberInfo member : metadataMembers) {
                        metadataEndpoints.add(member.toRaftEndpoint());
                    }
                    raftService.createRaftNode(metadataGroupId, metadataEndpoints, localCPMemberCandidate.toRaftEndpoint());
                }

                RaftOp op = new InitMetadataRaftGroupOp(localCPMemberCandidate, discoveredCPMembers, metadataGroupId.getSeed());
                raftService.getInvocationManager().invoke(metadataGroupId, op).get();
            } catch (Exception e) {
                logger.severe("Could not initialize METADATA CP group with CP members: " + metadataMembers, e);
                raftService.terminateRaftNode(metadataGroupId, true);
                return false;
            }
            return true;
        }

        private void handleDiscoveryFailure() {
            if (terminateOnDiscoveryFailure) {
                logger.warning("Terminating because of CP discovery failure...");
                terminateNode();
            } else {
                logger.warning("Cancelling CP Subsystem discovery...");
                discoveryCompleted.set(true);
            }
        }

        private void terminateNode() {
            nodeEngine.getNode().shutdown(true);
        }

        @SuppressWarnings("checkstyle:magicnumber")
        void cancelAndAwaitCompletion() {
            cancelled = true;
            while (state != DiscoveryTaskState.COMPLETED) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private CPMetadataStore getCpMetadataStore() {
        return raftService.getCPPersistenceService().getCPMetadataStore();
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
            if (o1.getName().equals(METADATA_CP_GROUP_NAME)) {
                return -1;
            } else if (o2.getName().equals(METADATA_CP_GROUP_NAME)) {
                return 1;
            } else if (o1.getName().equals(DEFAULT_GROUP_NAME)) {
                return -1;
            } else if (o2.getName().equals(DEFAULT_GROUP_NAME)) {
                return 1;
            }

            return o1.getName().compareTo(o2.getName());
        }
    }

}
