/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.MemberInfoUpdateOperation;
import com.hazelcast.internal.cluster.impl.operations.MemberRemoveOperation;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOperation;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOperation;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.SetUtil;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.version.ClusterVersion;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class ClusterServiceImpl implements ClusterService, ConnectionListener, ManagedService,
        EventPublishingService<MembershipEvent, MembershipListener>, TransactionalService {

    public static final String SERVICE_NAME = "hz:core:clusterService";

    static final String EXECUTOR_NAME = "hz:cluster";

    private static final int DEFAULT_MERGE_RUN_DELAY_MILLIS = 100;
    private static final int CLUSTER_EXECUTOR_QUEUE_CAPACITY = 1000;
    private static final long CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS = 1000;

    private static final String MEMBERSHIP_EVENT_EXECUTOR_NAME = "hz:cluster:event";

    private final Address thisAddress;

    private final Lock lock = new ReentrantLock();

    private final AtomicReference<MemberMap> memberMapRef = new AtomicReference<MemberMap>(MemberMap.empty());

    private final AtomicReference<MemberMap> membersRemovedInNotActiveStateRef
            = new AtomicReference<MemberMap>(MemberMap.empty());

    private final Node node;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    private final ClusterClockImpl clusterClock;

    private final ClusterStateManager clusterStateManager;

    private final ClusterJoinManager clusterJoinManager;

    private final ClusterHeartbeatManager clusterHeartbeatManager;

    private String clusterId;

    public ClusterServiceImpl(Node node) {

        this.node = node;
        nodeEngine = node.nodeEngine;

        logger = node.getLogger(ClusterService.class.getName());
        clusterClock = new ClusterClockImpl(logger);

        thisAddress = node.getThisAddress();

        clusterStateManager = new ClusterStateManager(node, lock);
        clusterJoinManager = new ClusterJoinManager(node, this, lock);
        clusterHeartbeatManager = new ClusterHeartbeatManager(node, this);

        registerThisMember();

        node.connectionManager.addConnectionListener(this);
        //MEMBERSHIP_EVENT_EXECUTOR is a single threaded executor to ensure that events are executed in correct order.
        nodeEngine.getExecutionService().register(MEMBERSHIP_EVENT_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
        registerMetrics();
    }

    private void registerThisMember() {
        MemberImpl thisMember = node.getLocalMember();
        setMembers(thisMember);
    }

    private void registerMetrics() {
        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        metricsRegistry.scanAndRegister(clusterClock, "cluster.clock");
        metricsRegistry.scanAndRegister(clusterHeartbeatManager, "cluster.heartbeat");
        metricsRegistry.scanAndRegister(this, "cluster");
    }

    @Override
    public ClusterClockImpl getClusterClock() {
        return clusterClock;
    }

    @Override
    public long getClusterTime() {
        return clusterClock.getClusterTime();
    }

    @Override
    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        if (this.clusterId == null) {
            this.clusterId = clusterId;
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        long mergeFirstRunDelayMs = node.getProperties().getMillis(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS);
        mergeFirstRunDelayMs = (mergeFirstRunDelayMs > 0 ? mergeFirstRunDelayMs : DEFAULT_MERGE_RUN_DELAY_MILLIS);

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(EXECUTOR_NAME, 2, CLUSTER_EXECUTOR_QUEUE_CAPACITY, ExecutorType.CACHED);

        long mergeNextRunDelayMs = node.getProperties().getMillis(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS);
        mergeNextRunDelayMs = (mergeNextRunDelayMs > 0 ? mergeNextRunDelayMs : DEFAULT_MERGE_RUN_DELAY_MILLIS);
        executionService.scheduleWithRepetition(EXECUTOR_NAME, new SplitBrainHandler(node), mergeFirstRunDelayMs,
                mergeNextRunDelayMs, TimeUnit.MILLISECONDS);

        clusterHeartbeatManager.init();
    }

    public void sendLocalMembershipEvent() {
        sendMembershipEvents(Collections.<MemberImpl>emptySet(), Collections.singleton(node.getLocalMember()));
    }

    public void sendMemberListToMember(Address target) {
        if (!isMaster()) {
            return;
        }
        if (thisAddress.equals(target)) {
            return;
        }
        MemberImpl member = getMember(target);
        String memberUuid = member != null ? member.getUuid() : null;
        Collection<MemberImpl> members = getMemberImpls();
        MemberInfoUpdateOperation op = new MemberInfoUpdateOperation(memberUuid, createMemberInfoList(members),
                                                                     clusterClock.getClusterTime(), null, false);
        nodeEngine.getOperationService().send(op, target);
    }

    public void removeAddress(Address deadAddress, String uuid, String reason) {
        lock.lock();
        try {
            MemberImpl member = getMember(deadAddress);
            if (member == null || !uuid.equals(member.getUuid())) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot remove " + deadAddress + ", either member is not present "
                            + "or uuid is not matching. Uuid: " + uuid + ", member: " + member);
                }
                return;
            }
            doRemoveAddress(deadAddress, reason, true);
        } finally {
            lock.unlock();
        }
    }

    public void removeAddress(Address deadAddress, String reason) {
        doRemoveAddress(deadAddress, reason, true);
    }

    void doRemoveAddress(Address deadAddress, String reason, boolean destroyConnection) {
        if (!ensureMemberIsRemovable(deadAddress)) {
            return;
        }

        lock.lock();
        try {
            if (deadAddress.equals(node.getMasterAddress())) {
                assignNewMaster();
            }
            if (node.isMaster()) {
                clusterJoinManager.removeJoin(deadAddress);
            }
            Connection conn = node.connectionManager.getConnection(deadAddress);
            if (destroyConnection && conn != null) {
                conn.close(reason, null);
            }
            MemberImpl deadMember = getMember(deadAddress);
            if (deadMember != null) {
                removeMember(deadMember);
                logger.info(membersString());
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean ensureMemberIsRemovable(Address deadAddress) {
        if (!node.joined()) {
            return false;
        }
        if (deadAddress.equals(thisAddress)) {
            return false;
        }
        return true;
    }

    private void assignNewMaster() {
        Address oldMasterAddress = node.getMasterAddress();
        if (node.joined()) {
            Collection<Member> members = getMembers();
            Member newMaster = null;
            int size = members.size();
            if (size > 1) {
                Iterator<Member> iterator = members.iterator();
                Member member = iterator.next();
                if (member.getAddress().equals(oldMasterAddress)) {
                    newMaster = iterator.next();
                } else {
                    logger.severe(format("Old master %s is dead, but the first of member list is a different member %s!",
                            oldMasterAddress, member));
                    newMaster = member;
                }
            } else {
                logger.warning(format("Old master %s is dead and this node is not master, "
                        + "but member list contains only %d members: %s", oldMasterAddress, size, members));
            }
            logger.info(format("Old master %s left the cluster, assigning new master %s", oldMasterAddress, newMaster));
            if (newMaster != null) {
                node.setMasterAddress(newMaster.getAddress());
            } else {
                node.setMasterAddress(null);
            }
        } else {
            node.setMasterAddress(null);
        }

        if (logger.isFineEnabled()) {
            logger.fine(format("Old master: %s, new master: %s ", oldMasterAddress, node.getMasterAddress()));
        }

        if (node.isMaster()) {
            clusterHeartbeatManager.resetMemberMasterConfirmations();
        } else {
            clusterHeartbeatManager.sendMasterConfirmation();
        }
    }

    public void merge(Address newTargetAddress) {
        node.getJoiner().setTargetAddress(newTargetAddress);
        LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
        lifecycleService.runUnderLifecycleLock(new ClusterMergeTask(node));
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            memberMapRef.set(MemberMap.singleton(node.getLocalMember()));
            clusterHeartbeatManager.reset();
            clusterStateManager.reset();
            clusterJoinManager.reset();
            membersRemovedInNotActiveStateRef.set(MemberMap.empty());
        } finally {
            lock.unlock();
        }
    }

    static List<MemberInfo> createMemberInfoList(Collection<MemberImpl> members) {
        List<MemberInfo> memberInfos = new LinkedList<MemberInfo>();
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member));
        }
        return memberInfos;
    }

    static Set<MemberInfo> createMemberInfoSet(Collection<MemberImpl> members) {
        Set<MemberInfo> memberInfos = SetUtil.createHashSet(members.size());
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member));
        }
        return memberInfos;
    }

    public boolean finalizeJoin(Collection<MemberInfo> members, Address callerAddress, String clusterId,
                                ClusterState clusterState, ClusterVersion clusterVersion,
                                long clusterStartTime, long masterTime) {
        lock.lock();
        try {
            if (!checkValidMaster(callerAddress)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Not finalizing join because caller: " + callerAddress + " is not known master: "
                            + getMasterAddress());
                }
                return false;
            }

            if (node.joined()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Node is already joined... No need to finalize join...");
                }

                return false;
            }

            initialClusterState(clusterState, clusterVersion);
            setClusterId(clusterId);
            ClusterClockImpl clusterClock = getClusterClock();
            clusterClock.setClusterStartTime(clusterStartTime);
            clusterClock.setMasterTime(masterTime);
            doUpdateMembers(members);
            clusterHeartbeatManager.heartbeat();

            node.setJoined();

            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean updateMembers(Collection<MemberInfo> members, Address callerAddress) {
        lock.lock();
        try {
            if (!checkValidMaster(callerAddress)) {
                logger.warning("Not updating members because caller: " + callerAddress  + " is not known master: "
                        + getMasterAddress());
                return false;
            }

            if (!node.joined()) {
                logger.warning("Not updating members received from caller: " + callerAddress +  " because node is not joined! ");
                return false;
            }

            if (!shouldProcessMemberUpdate(memberMapRef.get(), members)) {
                return false;
            }

            doUpdateMembers(members);
            return true;
        } finally {
            lock.unlock();
        }
    }

    private boolean checkValidMaster(Address callerAddress) {
        return  (callerAddress != null && callerAddress.equals(getMasterAddress()));
    }

    private void doUpdateMembers(Collection<MemberInfo> members) {
        MemberMap currentMemberMap = memberMapRef.get();

        String scopeId = thisAddress.getScopeId();
        Collection<MemberImpl> newMembers = new LinkedList<MemberImpl>();
        MemberImpl[] updatedMembers = new MemberImpl[members.size()];
        int memberIndex = 0;
        for (MemberInfo memberInfo : members) {
            Address address = memberInfo.getAddress();
            MemberImpl member = currentMemberMap.getMember(address);
            if (member == null) {
                member = createMember(memberInfo, scopeId);
                newMembers.add(member);
                long now = clusterClock.getClusterTime();
                clusterHeartbeatManager.onHeartbeat(member, now);
                clusterHeartbeatManager.acceptMasterConfirmation(member, now);

                repairPartitionTableIfReturningMember(member);

            }
            updatedMembers[memberIndex++] = member;
        }

        setMembers(updatedMembers);
        sendMembershipEvents(currentMemberMap.getMembers(), newMembers);

        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        membersRemovedInNotActiveStateRef.set(MemberMap.cloneExcluding(membersRemovedInNotActiveState, updatedMembers));

        clusterHeartbeatManager.heartbeat();
        logger.info(membersString());
    }

    private void repairPartitionTableIfReturningMember(MemberImpl member) {
        if (!isMaster()) {
            return;
        }

        if (getClusterState() == ClusterState.ACTIVE) {
            return;
        }

        if (!node.getNodeExtension().isStartCompleted()) {
            return;
        }

        Address address = member.getAddress();
        MemberImpl memberRemovedWhileClusterIsNotActive = getMemberRemovedWhileClusterIsNotActive(member.getUuid());
        if (memberRemovedWhileClusterIsNotActive != null) {
            Address oldAddress = memberRemovedWhileClusterIsNotActive.getAddress();
            if (!oldAddress.equals(address)) {
                assert !isMemberRemovedWhileClusterIsNotActive(address);

                logger.warning(member + " is returning with a new address. Old one was: " + oldAddress
                        + ". Will update partition table with the new address.");
                InternalPartitionServiceImpl partitionService = node.partitionService;
                partitionService.replaceAddress(oldAddress, address);
            }
        }
    }

    private boolean shouldProcessMemberUpdate(MemberMap currentMembers,
                                              Collection<MemberInfo> newMemberInfos) {
        int currentMembersSize = currentMembers.size();
        int newMembersSize = newMemberInfos.size();

        if (currentMembersSize > newMembersSize) {
            logger.warning("Received an older member update, no need to process...");
            nodeEngine.getOperationService().send(new TriggerMemberListPublishOperation(), getMasterAddress());
            return false;
        }

        // member-update process only accepts new member updates
        if (currentMembersSize == newMembersSize) {
            Set<MemberInfo> currentMemberInfos = createMemberInfoSet(currentMembers.getMembers());
            if (currentMemberInfos.containsAll(newMemberInfos)) {
                logger.fine("Received a periodic member update, no need to process...");
            } else {
                logger.warning("Received an inconsistent member update "
                        + "which contains new members and removes some of the current members! "
                        + "Ignoring and requesting a new member update...");
                nodeEngine.getOperationService().send(new TriggerMemberListPublishOperation(), getMasterAddress());
            }
            return false;
        }

        Set<MemberInfo> currentMemberInfos = createMemberInfoSet(currentMembers.getMembers());
        currentMemberInfos.removeAll(newMemberInfos);
        if (currentMemberInfos.isEmpty()) {
            return true;
        } else {
            logger.warning("Received an inconsistent member update."
                    + " It has more members but also removes some of the current members!"
                    + " Ignoring and requesting a new member update...");
            nodeEngine.getOperationService().send(new TriggerMemberListPublishOperation(), getMasterAddress());
            return false;
        }
    }

    private void sendMembershipEvents(Collection<MemberImpl> currentMembers, Collection<MemberImpl> newMembers) {
        Set<Member> eventMembers = new LinkedHashSet<Member>(currentMembers);
        if (!newMembers.isEmpty()) {
            if (newMembers.size() == 1) {
                MemberImpl newMember = newMembers.iterator().next();
                // sync call
                node.getPartitionService().memberAdded(newMember);

                // async events
                eventMembers.add(newMember);
                sendMembershipEventNotifications(newMember, unmodifiableSet(eventMembers), true);
            } else {
                for (MemberImpl newMember : newMembers) {
                    // sync call
                    node.getPartitionService().memberAdded(newMember);

                    // async events
                    eventMembers.add(newMember);
                    sendMembershipEventNotifications(newMember, unmodifiableSet(new LinkedHashSet<Member>(eventMembers)), true);
                }
            }
        }
    }

    public void updateMemberAttribute(String uuid, MemberAttributeOperationType operationType, String key, Object value) {
        lock.lock();
        try {
            MemberMap memberMap = memberMapRef.get();
            for (MemberImpl member : memberMap.getMembers()) {
                if (member.getUuid().equals(uuid)) {
                    if (!member.equals(getLocalMember())) {
                        member.updateAttribute(operationType, key, value);
                    }
                    sendMemberAttributeEvent(member, operationType, key, value);
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAdded(Connection connection) {
    }

    @Override
    public void connectionRemoved(Connection connection) {
        if (logger.isFineEnabled()) {
            logger.fine("Removed connection " + connection.getEndPoint());
        }
        if (!node.joined()) {
            Address masterAddress = node.getMasterAddress();
            if (masterAddress != null && masterAddress.equals(connection.getEndPoint())) {
                clusterJoinManager.setMasterAddress(null);
            }
        }
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    private void setMembers(MemberImpl... members) {
        if (members == null || members.length == 0) {
            return;
        }
        if (logger.isFineEnabled()) {
            logger.fine("Updating members " + Arrays.toString(members));
        }
        lock.lock();
        try {
            memberMapRef.set(MemberMap.createNew(members));
        } finally {
            lock.unlock();
        }
    }

    private void removeMember(MemberImpl deadMember) {
        logger.info("Removing " + deadMember);
        lock.lock();
        try {
            MemberMap currentMembers = memberMapRef.get();
            final Address deadAddress = deadMember.getAddress();

            if (currentMembers.contains(deadAddress)) {
                clusterHeartbeatManager.removeMember(deadMember);
                MemberMap newMembers = MemberMap.cloneExcluding(currentMembers, deadMember);
                memberMapRef.set(newMembers);

                if (node.isMaster()) {
                    if (logger.isFineEnabled()) {
                        logger.fine(deadMember + " is dead, sending remove to all other members...");
                    }
                    sendMemberRemoveOperation(deadMember);
                }

                final ClusterState clusterState = clusterStateManager.getState();
                if (clusterState != ClusterState.ACTIVE) {
                    if (logger.isFineEnabled()) {
                        logger.fine(deadMember + " is dead, added to members left while cluster is " + clusterState + " state");
                    }

                    final InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
                    if (!hotRestartService.isMemberExcluded(deadAddress, deadMember.getUuid())) {
                        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
                        membersRemovedInNotActiveStateRef
                                .set(MemberMap.cloneAdding(membersRemovedInNotActiveState, deadMember));
                    }

                    InternalPartitionServiceImpl partitionService = node.partitionService;
                    partitionService.cancelReplicaSyncRequestsTo(deadAddress);
                } else {
                    onMemberRemove(deadMember, newMembers);
                }

                // async events
                sendMembershipEventNotifications(deadMember,
                        unmodifiableSet(new LinkedHashSet<Member>(newMembers.getMembers())), false);
            }
        } finally {
            lock.unlock();
        }
    }

    private void onMemberRemove(MemberImpl deadMember, MemberMap newMembers) {
        // sync call
        node.getPartitionService().memberRemoved(deadMember);
        // sync call
        nodeEngine.onMemberLeft(deadMember);
    }

    public boolean isMemberRemovedWhileClusterIsNotActive(Address target) {
        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        return membersRemovedInNotActiveState.contains(target);
    }

    boolean isMemberRemovedWhileClusterIsNotActive(String uuid) {
        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        return membersRemovedInNotActiveState.contains(uuid);
    }

    MemberImpl getMemberRemovedWhileClusterIsNotActive(String uuid) {
        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        return membersRemovedInNotActiveState.getMember(uuid);
    }

    public Collection<Member> getCurrentMembersAndMembersRemovedWhileClusterIsNotActive() {
        lock.lock();
        try {
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            if (membersRemovedInNotActiveState.size() == 0) {
                return getMembers();
            }

            Collection<MemberImpl> removedMembers = membersRemovedInNotActiveState.getMembers();
            Collection<MemberImpl> members = memberMapRef.get().getMembers();

            Collection<Member> allMembers = new ArrayList<Member>(members.size() + removedMembers.size());
            allMembers.addAll(members);
            allMembers.addAll(removedMembers);

            return allMembers;
        } finally {
            lock.unlock();
        }
    }

    void removeMembersDeadWhileClusterIsNotActive() {
        lock.lock();
        try {
            MemberMap memberMap = memberMapRef.get();
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            Collection<MemberImpl> members = membersRemovedInNotActiveState.getMembers();
            membersRemovedInNotActiveStateRef.set(MemberMap.empty());
            for (MemberImpl member : members) {
                onMemberRemove(member, memberMap);
            }

        } finally {
            lock.unlock();
        }
    }

    public void notifyForRemovedMember(MemberImpl member) {
        lock.lock();
        try {
            MemberMap memberMap = memberMapRef.get();
            onMemberRemove(member, memberMap);
        } finally {
            lock.unlock();
        }
    }

    public void shrinkMembersRemovedWhileClusterIsNotActiveState(Collection<String> memberUuidsToRemove) {
        lock.lock();
        try {
            Set<MemberImpl> membersRemovedInNotActiveState
                    = new LinkedHashSet<MemberImpl>(membersRemovedInNotActiveStateRef.get().getMembers());

            Iterator<MemberImpl> it = membersRemovedInNotActiveState.iterator();
            while (it.hasNext()) {
                MemberImpl member = it.next();
                if (memberUuidsToRemove.contains(member.getUuid())) {
                    logger.fine("Removing " + member + " from members removed while in cluster not active state");
                    it.remove();
                }
            }
            membersRemovedInNotActiveStateRef.set(MemberMap.createNew(membersRemovedInNotActiveState.toArray(new MemberImpl[0])));
        } finally {
            lock.unlock();
        }
    }

    private void sendMemberRemoveOperation(Member deadMember) {
        for (Member member : getMembers()) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address) && !address.equals(deadMember.getAddress())) {
                MemberRemoveOperation op = new MemberRemoveOperation(deadMember.getAddress(), deadMember.getUuid());
                nodeEngine.getOperationService().send(op, address);
            }
        }
    }

    public void sendShutdownMessage() {
        sendMemberRemoveOperation(getLocalMember());
    }

    private void sendMembershipEventNotifications(MemberImpl member, Set<Member> members, final boolean added) {
        int eventType = added ? MembershipEvent.MEMBER_ADDED : MembershipEvent.MEMBER_REMOVED;
        MembershipEvent membershipEvent = new MembershipEvent(this, member, eventType, members);
        Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        if (membershipAwareServices != null && !membershipAwareServices.isEmpty()) {
            final MembershipServiceEvent event = new MembershipServiceEvent(membershipEvent);
            for (final MembershipAwareService service : membershipAwareServices) {
                nodeEngine.getExecutionService().execute(MEMBERSHIP_EVENT_EXECUTOR_NAME, new Runnable() {
                    public void run() {
                        if (added) {
                            service.memberAdded(event);
                        } else {
                            service.memberRemoved(event);
                        }
                    }
                });
            }
        }
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        for (EventRegistration reg : registrations) {
            eventService.publishEvent(SERVICE_NAME, reg, membershipEvent, reg.getId().hashCode());
        }
    }

    private void sendMemberAttributeEvent(MemberImpl member, MemberAttributeOperationType operationType, String key,
                                          Object value) {
        final MemberAttributeServiceEvent event
                = new MemberAttributeServiceEvent(this, member, operationType, key, value);
        MemberAttributeEvent attributeEvent = new MemberAttributeEvent(this, member, operationType, key, value);
        Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        if (membershipAwareServices != null && !membershipAwareServices.isEmpty()) {
            for (final MembershipAwareService service : membershipAwareServices) {
                // service events should not block each other
                nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                    public void run() {
                        service.memberAttributeChanged(event);
                    }
                });
            }
        }
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        for (EventRegistration reg : registrations) {
            eventService.publishEvent(SERVICE_NAME, reg, attributeEvent, reg.getId().hashCode());
        }
    }

    private MemberImpl createMember(MemberInfo memberInfo, String ipV6ScopeId) {
        Address address = memberInfo.getAddress();
        address.setScopeId(ipV6ScopeId);
        return new MemberImpl(address, memberInfo.getVersion(), thisAddress.equals(address), memberInfo.getUuid(),
                (HazelcastInstanceImpl) nodeEngine.getHazelcastInstance(), memberInfo.getAttributes(), memberInfo.isLiteMember());
    }

    @Override
    public MemberImpl getMember(Address address) {
        if (address == null) {
            return null;
        }
        MemberMap memberMap = memberMapRef.get();
        return memberMap.getMember(address);
    }

    @Override
    public MemberImpl getMember(String uuid) {
        if (uuid == null) {
            return null;
        }

        MemberMap memberMap = memberMapRef.get();
        return memberMap.getMember(uuid);
    }

    @Override
    public Collection<MemberImpl> getMemberImpls() {
        return memberMapRef.get().getMembers();
    }

    public Collection<Address> getMemberAddresses() {
        return memberMapRef.get().getAddresses();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Member> getMembers() {
        return (Set) memberMapRef.get().getMembers();
    }

    @Override
    public Collection<Member> getMembers(MemberSelector selector) {
        return (Collection) new MemberSelectingCollection(memberMapRef.get().getMembers(), selector);
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public Address getMasterAddress() {
        return node.getMasterAddress();
    }

    @Override
    public boolean isMaster() {
        return node.isMaster();
    }

    @Override
    public Address getThisAddress() {
        return thisAddress;
    }

    public Member getLocalMember() {
        return node.getLocalMember();
    }

    @Probe(name = "size")
    @Override
    public int getSize() {
        return getMembers().size();
    }

    @Override
    public int getSize(MemberSelector selector) {
        int size = 0;
        for (MemberImpl member : memberMapRef.get().getMembers()) {
            if (selector.select(member)) {
                size++;
            }
        }

        return size;
    }

    public String addMembershipListener(MembershipListener listener) {
        checkNotNull(listener, "listener cannot be null");

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration;
        if (listener instanceof InitialMembershipListener) {
            lock.lock();
            try {
                ((InitialMembershipListener) listener).init(new InitialMembershipEvent(this, getMembers()));
                registration = eventService.registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
            } finally {
                lock.unlock();
            }
        } else {
            registration = eventService.registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
        }

        return registration.getId();
    }

    public boolean removeMembershipListener(String registrationId) {
        checkNotNull(registrationId, "registrationId cannot be null");

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    @Override
    public void dispatchEvent(MembershipEvent event, MembershipListener listener) {
        switch (event.getEventType()) {
            case MembershipEvent.MEMBER_ADDED:
                listener.memberAdded(event);
                break;
            case MembershipEvent.MEMBER_REMOVED:
                listener.memberRemoved(event);
                break;
            case MembershipEvent.MEMBER_ATTRIBUTE_CHANGED:
                MemberAttributeEvent memberAttributeEvent = (MemberAttributeEvent) event;
                listener.memberAttributeChanged(memberAttributeEvent);
                break;
            default:
                throw new IllegalArgumentException("Unhandled event: " + event);
        }
    }

    public String membersString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        Collection<MemberImpl> members = getMemberImpls();
        sb.append(members != null ? members.size() : 0);
        sb.append("] {");
        if (members != null) {
            for (Member member : members) {
                sb.append("\n\t").append(member);
            }
        }
        sb.append("\n}\n");
        return sb.toString();
    }

    @Override
    public ClusterState getClusterState() {
        return clusterStateManager.getState();
    }

    @Override
    public <T extends TransactionalObject> T createTransactionalObject(String name, Transaction transaction) {
        throw new UnsupportedOperationException(SERVICE_NAME + " does not support TransactionalObjects!");
    }

    @Override
    public void rollbackTransaction(String transactionId) {
        logger.info("Rolling back cluster state. Transaction: " + transactionId);
        clusterStateManager.rollbackClusterState(transactionId);
    }

    @Override
    public void changeClusterState(ClusterState newState) {
        changeClusterState(newState, false);
    }

    private void changeClusterState(ClusterState newState, boolean isTransient) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(newState), getMembers(), partitionStateVersion,
                isTransient);
    }

    @Override
    public void changeClusterState(ClusterState newState, TransactionOptions options) {
        changeClusterState(newState, options, false);
    }

    private void changeClusterState(ClusterState newState, TransactionOptions options, boolean isTransient) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(newState), getMembers(), options, partitionStateVersion,
                isTransient);
    }

    @Override
    public ClusterVersion getClusterVersion() {
        return clusterStateManager.getClusterVersion();
    }

    @Override
    public HotRestartService getHotRestartService() {
        return node.getNodeExtension().getHotRestartService();
    }

    @Override
    public void changeClusterVersion(ClusterVersion version) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(version), getMembers(), partitionStateVersion, false);
    }

    @Override
    public void changeClusterVersion(ClusterVersion version, TransactionOptions options) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(version), getMembers(), options, partitionStateVersion,
                false);
    }

    void addMembersRemovedInNotActiveState(Collection<MemberImpl> members) {
        lock.lock();
        try {
            members.remove(node.getLocalMember());
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            membersRemovedInNotActiveStateRef.set(MemberMap.cloneAdding(membersRemovedInNotActiveState,
                    members.toArray(new MemberImpl[0])));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
        changeClusterState(ClusterState.PASSIVE, true);
        shutdownNodes();
    }

    @Override
    public void shutdown(TransactionOptions options) {
        changeClusterState(ClusterState.PASSIVE, options, true);
        shutdownNodes();
    }

    private void shutdownNodes() {
        final Operation op = new ShutdownNodeOperation();

        logger.info("Sending shutting down operations to all members...");

        Collection<Member> members = getMembers(NON_LOCAL_MEMBER_SELECTOR);
        final long timeout = node.getProperties().getNanos(GroupProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS);
        final long startTime = System.nanoTime();

        while ((System.nanoTime() - startTime) < timeout && !members.isEmpty()) {
            for (Member member : members) {
                nodeEngine.getOperationService().send(op, member.getAddress());
            }

            try {
                Thread.sleep(CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("Shutdown sleep interrupted. ", e);
                break;
            }

            members = getMembers(NON_LOCAL_MEMBER_SELECTOR);
        }

        logger.info("Number of other nodes remaining: " + getSize(NON_LOCAL_MEMBER_SELECTOR) + ". Shutting down itself.");

        final HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
        hazelcastInstance.getLifecycleService().shutdown();
    }

    void initialClusterState(ClusterState clusterState, ClusterVersion version) {
        if (node.joined()) {
            throw new IllegalStateException("Cannot set initial state after node joined! -> " + clusterState);
        }
        clusterStateManager.initialClusterState(clusterState, version);
    }

    public ClusterStateManager getClusterStateManager() {
        return clusterStateManager;
    }

    public ClusterJoinManager getClusterJoinManager() {
        return clusterJoinManager;
    }

    public ClusterHeartbeatManager getClusterHeartbeatManager() {
        return clusterHeartbeatManager;
    }

    @Override
    public String toString() {
        return "ClusterService"
                + "{address=" + thisAddress
                + '}';
    }
}
