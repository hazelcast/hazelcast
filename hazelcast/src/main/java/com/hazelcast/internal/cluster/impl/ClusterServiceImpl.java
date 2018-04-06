/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.operations.ExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.cluster.impl.operations.PromoteLiteMemberOp;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerExplicitSuspicionOp;
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
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.version.Version;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.instance.MemberImpl.NA_MEMBER_LIST_JOIN_VERSION;
import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class ClusterServiceImpl implements ClusterService, ConnectionListener, ManagedService,
        EventPublishingService<MembershipEvent, MembershipListener>, TransactionalService {

    public static final String SERVICE_NAME = "hz:core:clusterService";

    static final String EXECUTOR_NAME = "hz:cluster";
    static final String MEMBERSHIP_EVENT_EXECUTOR_NAME = "hz:cluster:event";

    private static final int DEFAULT_MERGE_RUN_DELAY_MILLIS = 100;
    private static final int CLUSTER_EXECUTOR_QUEUE_CAPACITY = 1000;
    private static final long CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS = 1000;
    private static final boolean ASSERTION_ENABLED = ClusterServiceImpl.class.desiredAssertionStatus();

    private final ReentrantLock lock = new ReentrantLock();

    private final Node node;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    private final ClusterClockImpl clusterClock;

    private final MembershipManager membershipManager;

    private final ClusterStateManager clusterStateManager;

    private final ClusterJoinManager clusterJoinManager;

    private final ClusterHeartbeatManager clusterHeartbeatManager;

    private final AtomicBoolean joined = new AtomicBoolean(false);

    private final boolean useLegacyMemberListFormat;

    private volatile MemberImpl localMember;

    private volatile Address masterAddress;

    private volatile String clusterId;


    public ClusterServiceImpl(Node node, MemberImpl localMember) {
        this.node = node;
        this.localMember = localMember;
        nodeEngine = node.nodeEngine;

        logger = node.getLogger(ClusterService.class.getName());
        clusterClock = new ClusterClockImpl(logger);

        useLegacyMemberListFormat = node.getProperties().getBoolean(GroupProperty.USE_LEGACY_MEMBER_LIST_FORMAT);

        membershipManager = new MembershipManager(node, this, lock);
        clusterStateManager = new ClusterStateManager(node, lock);
        clusterJoinManager = new ClusterJoinManager(node, this, lock);
        clusterHeartbeatManager = new ClusterHeartbeatManager(node, this, lock);

        node.connectionManager.addConnectionListener(this);
        //MEMBERSHIP_EVENT_EXECUTOR is a single threaded executor to ensure that events are executed in correct order.
        nodeEngine.getExecutionService().register(MEMBERSHIP_EVENT_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
        registerMetrics();
    }

    private void registerMetrics() {
        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        metricsRegistry.scanAndRegister(clusterClock, "cluster.clock");
        metricsRegistry.scanAndRegister(clusterHeartbeatManager, "cluster.heartbeat");
        metricsRegistry.scanAndRegister(this, "cluster");
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

        membershipManager.init();
        clusterHeartbeatManager.init();
    }

    public void sendLocalMembershipEvent() {
        membershipManager.sendMembershipEvents(Collections.<MemberImpl>emptySet(), Collections.singleton(getLocalMember()));
    }

    public void handleExplicitSuspicion(MembersViewMetadata expectedMembersViewMetadata, Address suspectedAddress) {
        membershipManager.handleExplicitSuspicion(expectedMembersViewMetadata, suspectedAddress);
    }

    public void handleExplicitSuspicionTrigger(Address caller, int callerMemberListVersion,
                                               MembersViewMetadata suspectedMembersViewMetadata) {
        membershipManager.handleExplicitSuspicionTrigger(caller, callerMemberListVersion, suspectedMembersViewMetadata);
    }

    public void suspectMember(Member suspectedMember, String reason, boolean destroyConnection) {
        membershipManager.suspectMember((MemberImpl) suspectedMember, reason, destroyConnection);
    }

    public void suspectAddressIfNotConnected(Address address) {
        lock.lock();
        try {
            MemberImpl member = getMember(address);
            if (member == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot suspect " + address + ", since it's not a member.");
                }

                return;
            }

            Connection conn = node.getConnectionManager().getConnection(address);
            if (conn != null && conn.isAlive()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot suspect " + member + ", since there's a live connection -> " + conn);
                }

                return;
            }
            suspectMember(member, "No connection", false);
        } finally {
            lock.unlock();
        }
    }

    void sendExplicitSuspicion(MembersViewMetadata endpointMembersViewMetadata) {
        Address endpoint = endpointMembersViewMetadata.getMemberAddress();
        if (endpoint.equals(node.getThisAddress())) {
            logger.warning("Cannot send explicit suspicion for " + endpointMembersViewMetadata + " to itself.");
            return;
        }

        if (!isJoined()) {
            if (logger.isFineEnabled()) {
                logger.fine("Cannot send explicit suspicion, not joined yet!");
            }

            return;
        }

        Version clusterVersion = getClusterVersion();
        assert !clusterVersion.isUnknown() : "Cluster version should not be unknown after join!";

        Operation op = new ExplicitSuspicionOp(endpointMembersViewMetadata);
        nodeEngine.getOperationService().send(op, endpoint);
    }

    void sendExplicitSuspicionTrigger(Address triggerTo, MembersViewMetadata endpointMembersViewMetadata) {
        if (triggerTo.equals(node.getThisAddress())) {
            logger.warning("Cannot send explicit suspicion trigger for " + endpointMembersViewMetadata + " to itself.");
            return;
        }

        int memberListVersion = membershipManager.getMemberListVersion();
        Operation op = new TriggerExplicitSuspicionOp(memberListVersion, endpointMembersViewMetadata);
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(op, triggerTo);
    }

    public MembersView handleMastershipClaim(Address candidateAddress, String candidateUuid) {
        checkNotNull(candidateAddress);
        checkNotNull(candidateUuid);
        checkFalse(getThisAddress().equals(candidateAddress), "cannot accept my own mastership claim!");

        lock.lock();
        try {
            checkTrue(isJoined(), candidateAddress + " claims mastership but this node is not joined!");
            checkFalse(isMaster(),
                    candidateAddress + " claims mastership but this node is master!");

            MemberImpl masterCandidate = membershipManager.getMember(candidateAddress, candidateUuid);
            checkTrue(masterCandidate != null,
                    candidateAddress + " claims mastership but it is not a member!");

            MemberMap memberMap = membershipManager.getMemberMap();
            if (!shouldAcceptMastership(memberMap, masterCandidate)) {
                String message = "Cannot accept mastership claim of " + candidateAddress
                        + " at the moment. There are more suitable master candidates in the member list.";
                logger.fine(message);
                throw new RetryableHazelcastException(message);
            }

            if (!membershipManager.clearMemberSuspicion(candidateAddress, "Mastership claim")) {
                throw new IllegalStateException("Cannot accept mastership claim of " + candidateAddress + ". "
                        + getMasterAddress() + " is already master.");
            }

            setMasterAddress(masterCandidate.getAddress());

            MembersView response = memberMap.toTailMembersView(masterCandidate, true);

            logger.warning("Mastership of " + candidateAddress + " is accepted. Response: " + response);

            return response;
        } finally {
            lock.unlock();
        }
    }

    // called under cluster service lock
    // mastership is accepted when all members before the candidate is suspected
    private boolean shouldAcceptMastership(MemberMap memberMap, MemberImpl candidate) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        for (MemberImpl member : memberMap.headMemberSet(candidate, false)) {
            if (!membershipManager.isMemberSuspected(member.getAddress())) {
                if (logger.isFineEnabled()) {
                    logger.fine("Should not accept mastership claim of " + candidate + ", because " + member
                            + " is not suspected at the moment and is before than " + candidate + " in the member list.");
                }

                return false;
            }
        }
        return true;
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
            resetJoinState();
            resetLocalMemberUuid();
            resetClusterId();
            clearInternalState();
        } finally {
            lock.unlock();
        }
    }

    private void resetLocalMemberUuid() {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        assert !isJoined() : "Cannot reset local member UUID when joined.";

        Address address = getThisAddress();
        String newUuid = UuidUtil.createMemberUuid(address);

        logger.warning("Resetting local member UUID. Previous: " + localMember.getUuid() + ", new: " + newUuid);

        MemberImpl memberWithNewUuid = new MemberImpl(address, localMember.getVersion(),
                true, newUuid, localMember.getAttributes(), localMember.isLiteMember(),
                localMember.getMemberListJoinVersion(), node.hazelcastInstance);

        localMember = memberWithNewUuid;

        node.loggingService.setThisMember(localMember);
    }

    public void resetJoinState() {
        lock.lock();
        try {
            setMasterAddress(null);
            setJoined(false);
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public boolean finalizeJoin(MembersView membersView, Address callerAddress, String callerUuid, String targetUuid,
                                String clusterId, ClusterState clusterState, Version clusterVersion, long clusterStartTime,
                                long masterTime, OnJoinOp preJoinOp) {
        lock.lock();
        try {
            if (!checkValidMaster(callerAddress)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Not finalizing join because caller: " + callerAddress + " is not known master: "
                            + getMasterAddress());
                }
                MembersViewMetadata membersViewMetadata = new MembersViewMetadata(callerAddress, callerUuid,
                        callerAddress, membersView.getVersion());
                sendExplicitSuspicion(membersViewMetadata);
                return false;
            }

            if (isJoined()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Node is already joined... No need to finalize join...");
                }

                return false;
            }

            checkMemberUpdateContainsLocalMember(membersView, targetUuid);

            initialClusterState(clusterState, clusterVersion);
            setClusterId(clusterId);
            ClusterClockImpl clusterClock = getClusterClock();
            clusterClock.setClusterStartTime(clusterStartTime);
            clusterClock.setMasterTime(masterTime);

            // run pre-join op before member list update, so operations other than join ops will be refused by operation service
            if (preJoinOp != null) {
                nodeEngine.getOperationService().run(preJoinOp);
            }

            membershipManager.updateMembers(membersView);
            clusterHeartbeatManager.heartbeat();
            setJoined(true);

            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean updateMembers(MembersView membersView, Address callerAddress, String callerUuid, String targetUuid) {
        lock.lock();
        try {
            if (!isJoined()) {
                logger.warning("Not updating members received from caller: " + callerAddress + " because node is not joined! ");
                return false;
            }

            if (!checkValidMaster(callerAddress)) {
                logger.warning("Not updating members because caller: " + callerAddress + " is not known master: "
                        + getMasterAddress());
                MembersViewMetadata callerMembersViewMetadata = new MembersViewMetadata(callerAddress, callerUuid,
                        callerAddress, membersView.getVersion());
                if (!clusterJoinManager.isMastershipClaimInProgress()) {
                    sendExplicitSuspicion(callerMembersViewMetadata);
                }
                return false;
            }

            checkMemberUpdateContainsLocalMember(membersView, targetUuid);

            if (!shouldProcessMemberUpdate(membersView)) {
                return false;
            }

            membershipManager.updateMembers(membersView);
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void checkMemberUpdateContainsLocalMember(MembersView membersView, String targetUuid) {
        String thisUuid = getThisUuid();
        if (!thisUuid.equals(targetUuid)) {
            String msg = "Not applying member update because target uuid: " + targetUuid + " is different! -> " + membersView
                    + ", local member: " + localMember;
            throw new IllegalArgumentException(msg);
        }

        Member localMember = getLocalMember();
        if (!membersView.containsMember(localMember.getAddress(), localMember.getUuid())) {
            String msg = "Not applying member update because member list doesn't contain us! -> " + membersView
                    + ", local member: " + localMember;
            throw new IllegalArgumentException(msg);
        }
    }

    private boolean checkValidMaster(Address callerAddress) {
        return (callerAddress != null && callerAddress.equals(getMasterAddress()));
    }

    void repairPartitionTableIfReturningMember(MemberImpl member) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        if (!isMaster()) {
            return;
        }

        if (getClusterState().isMigrationAllowed()) {
            return;
        }

        if (!node.getNodeExtension().isStartCompleted()) {
            return;
        }

        Address address = member.getAddress();
        MemberImpl memberRemovedWhileClusterIsNotActive
                = membershipManager.getMemberRemovedInNotJoinableState(member.getUuid());
        if (memberRemovedWhileClusterIsNotActive != null) {
            Address oldAddress = memberRemovedWhileClusterIsNotActive.getAddress();
            if (!oldAddress.equals(address)) {
                assert !isMemberRemovedInNotJoinableState(address);

                logger.warning(member + " is returning with a new address. Old one was: " + oldAddress
                        + ". Will update partition table with the new address.");
                InternalPartitionServiceImpl partitionService = node.partitionService;
                partitionService.replaceAddress(oldAddress, address);
            }
        }
    }

    private boolean shouldProcessMemberUpdate(MembersView membersView) {
        int memberListVersion = membershipManager.getMemberListVersion();
        if (memberListVersion > membersView.getVersion()) {
            if (logger.isFineEnabled()) {
                logger.fine("Received an older member update, ignoring... Current version: "
                        + memberListVersion + ", Received version: " + membersView.getVersion());
            }

            return false;
        }

        if (memberListVersion == membersView.getVersion()) {
            if (ASSERTION_ENABLED) {
                MemberMap memberMap = membershipManager.getMemberMap();
                Collection<Address> currentAddresses = memberMap.getAddresses();
                Collection<Address> newAddresses = membersView.getAddresses();

                assert currentAddresses.size() == newAddresses.size()
                        && newAddresses.containsAll(currentAddresses)
                        : "Member view versions are same but new member view doesn't match the current!"
                        + " Current: " + memberMap.toMembersView() + ", New: " + membersView;
            }

            if (logger.isFineEnabled()) {
                logger.fine("Received a periodic member update, ignoring... Version: " + memberListVersion);
            }

            return false;
        }

        return true;
    }

    public void updateMemberAttribute(String uuid, MemberAttributeOperationType operationType, String key, Object value) {
        lock.lock();
        try {
            MemberImpl member = membershipManager.getMember(uuid);
            if (!member.equals(getLocalMember())) {
                member.updateAttribute(operationType, key, value);
            }
            sendMemberAttributeEvent(member, operationType, key, value);
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
            logger.fine("Removed connection to " + connection.getEndPoint());
        }
        if (!isJoined()) {
            Address masterAddress = getMasterAddress();
            if (masterAddress != null && masterAddress.equals(connection.getEndPoint())) {
                setMasterAddressToJoin(null);
            }
        }
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    public boolean isMemberRemovedInNotJoinableState(Address target) {
        return membershipManager.isMemberRemovedInNotJoinableState(target);
    }

    boolean isMemberRemovedInNotJoinableState(String uuid) {
        return membershipManager.isMemberRemovedInNotJoinableState(uuid);
    }

    public Collection<Member> getCurrentMembersAndMembersRemovedInNotJoinableState() {
        return membershipManager.getCurrentMembersAndMembersRemovedInNotJoinableState();
    }

    public void notifyForRemovedMember(MemberImpl member) {
        lock.lock();
        try {
            membershipManager.onMemberRemove(member);
        } finally {
            lock.unlock();
        }
    }

    public void shrinkMembersRemovedInNotJoinableState(Collection<String> memberUuidsToRemove) {
        membershipManager.shrinkMembersRemovedInNotJoinableState(memberUuidsToRemove);
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
                nodeEngine.getExecutionService().execute(SYSTEM_EXECUTOR, new Runnable() {
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

    @Override
    public MemberImpl getMember(Address address) {
        if (address == null) {
            return null;
        }
        return membershipManager.getMember(address);
    }

    @Override
    public MemberImpl getMember(String uuid) {
        if (uuid == null) {
            return null;
        }
        return membershipManager.getMember(uuid);
    }

    @Override
    public Collection<MemberImpl> getMemberImpls() {
        return membershipManager.getMembers();
    }

    public Collection<Address> getMemberAddresses() {
        return membershipManager.getMemberMap().getAddresses();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Member> getMembers() {
        return membershipManager.getMemberSet();
    }

    @Override
    public Collection<Member> getMembers(MemberSelector selector) {
        return (Collection) new MemberSelectingCollection(membershipManager.getMembers(), selector);
    }

    @Override
    public void shutdown(boolean terminate) {
        clearInternalState();
    }

    private void clearInternalState() {
        lock.lock();
        try {
            membershipManager.reset();
            clusterHeartbeatManager.reset();
            clusterStateManager.reset();
            clusterJoinManager.reset();
            resetJoinState();
        } finally {
            lock.unlock();
        }
    }

    public boolean setMasterAddressToJoin(final Address master) {
        lock.lock();
        try {
            if (isJoined()) {
                Address currentMasterAddress = getMasterAddress();
                if (!currentMasterAddress.equals(master)) {
                    logger.warning("Cannot set master address to " + master
                            + " because node is already joined! Current master: " + currentMasterAddress);
                } else if (logger.isFineEnabled()) {
                    logger.fine("Master address is already set to " + master);
                }
                return false;
            }

            setMasterAddress(master);
            return true;
        } finally {
            lock.unlock();
        }
    }

    // should be called under lock
    void setMasterAddress(Address master) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        if (master != null) {
            if (logger.isFineEnabled()) {
                logger.fine("Setting master address to " + master);
            }
        }
        masterAddress = master;
    }

    @Override
    public Address getMasterAddress() {
        return masterAddress;
    }

    @Override
    public boolean isMaster() {
        return node.getThisAddress().equals(masterAddress);
    }

    @Override
    public Address getThisAddress() {
        return node.getThisAddress();
    }

    @Override
    public MemberImpl getLocalMember() {
        return localMember;
    }

    public String getThisUuid() {
        return localMember.getUuid();
    }

    // should be called under lock
    void setJoined(boolean val) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        joined.set(val);
    }

    @Override
    public boolean isJoined() {
        return joined.get();
    }

    @Probe
    @Override
    public int getSize() {
        return membershipManager.getMemberMap().size();
    }

    @Override
    public int getSize(MemberSelector selector) {
        int size = 0;
        for (MemberImpl member : membershipManager.getMembers()) {
            if (selector.select(member)) {
                size++;
            }
        }

        return size;
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

    // called under cluster service lock
    void setClusterId(String newClusterId) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        assert clusterId == null : "Cluster ID should be null: " + clusterId;
        clusterId = newClusterId;
    }

    // called under cluster service lock
    private void resetClusterId() {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        clusterId = null;
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

    private String legacyMemberListString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        Collection<MemberImpl> members = getMemberImpls();
        sb.append(members.size());
        sb.append("] {");
        for (Member member : members) {
            sb.append("\n\t").append(member);
        }
        sb.append("\n}\n");
        return sb.toString();
    }

    public String getMemberListString() {
        return useLegacyMemberListFormat ? legacyMemberListString() : membershipManager.memberListString();
    }

    void printMemberList() {
        logger.info(getMemberListString());
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
        clusterStateManager.rollbackClusterState(transactionId);
    }

    @Override
    public void changeClusterState(ClusterState newState) {
        changeClusterState(newState, false);
    }

    private void changeClusterState(ClusterState newState, boolean isTransient) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(newState), membershipManager.getMemberMap(),
                partitionStateVersion, isTransient);
    }

    @Override
    public void changeClusterState(ClusterState newState, TransactionOptions options) {
        changeClusterState(newState, options, false);
    }

    private void changeClusterState(ClusterState newState, TransactionOptions options, boolean isTransient) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(newState), membershipManager.getMemberMap(),
                options, partitionStateVersion, isTransient);
    }

    @Override
    public Version getClusterVersion() {
        return clusterStateManager.getClusterVersion();
    }

    @Override
    public HotRestartService getHotRestartService() {
        return node.getNodeExtension().getHotRestartService();
    }

    @Override
    public void changeClusterVersion(Version version) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(version), membershipManager.getMemberMap(),
                partitionStateVersion, false);
    }

    @Override
    public void changeClusterVersion(Version version, TransactionOptions options) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(version), membershipManager.getMemberMap(),
                options, partitionStateVersion, false);
    }

    @Override
    public int getMemberListJoinVersion() {
        lock.lock();
        try {
            if (!isJoined()) {
                throw new IllegalStateException("Member list join version is not available when not joined");
            } else if (getClusterVersion().isLessThan(V3_10)) {
                // RU_COMPAT_3_9
                String msg = "Member list join version is not available with a cluster version less than 3.10";
                throw new UnsupportedOperationException(msg);
            }

            int joinVersion = localMember.getMemberListJoinVersion();
            if (joinVersion == NA_MEMBER_LIST_JOIN_VERSION) {
                // This can happen when the cluster was just upgraded to 3.10, but this member did not yet learn
                // its node ID by an async call from master.
                throw new IllegalStateException("Member list join version is not yet available");
            }
            return joinVersion;
        } finally {
            lock.unlock();
        }
    }

    void addMembersRemovedInNotJoinableState(Collection<MemberImpl> members) {
        membershipManager.addMembersRemovedInNotJoinableState(members);
    }

    @Override
    public void shutdown() {
        shutdownCluster(null);
    }

    @Override
    public void shutdown(TransactionOptions options) {
        shutdownCluster(options);
    }

    private void shutdownCluster(TransactionOptions options) {
        if (options == null) {
            changeClusterState(ClusterState.PASSIVE, true);
        } else {
            changeClusterState(ClusterState.PASSIVE, options, true);
        }

        long timeoutNanos = node.getProperties().getNanos(GroupProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS);
        long startNanos = System.nanoTime();
        node.getNodeExtension().getInternalHotRestartService()
                .waitPartitionReplicaSyncOnCluster(timeoutNanos, TimeUnit.NANOSECONDS);
        timeoutNanos -= (System.nanoTime() - startNanos);
        shutdownNodes(timeoutNanos);
    }

    private void shutdownNodes(final long timeoutNanos) {
        final Operation op = new ShutdownNodeOp();

        logger.info("Sending shutting down operations to all members...");

        Collection<Member> members = getMembers(NON_LOCAL_MEMBER_SELECTOR);
        final long startTime = System.nanoTime();

        while ((System.nanoTime() - startTime) < timeoutNanos && !members.isEmpty()) {
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

    private void initialClusterState(ClusterState clusterState, Version version) {
        if (isJoined()) {
            throw new IllegalStateException("Cannot set initial state after node joined! -> " + clusterState);
        }
        clusterStateManager.initialClusterState(clusterState, version);
    }

    public MembershipManager getMembershipManager() {
        return membershipManager;
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
    public void promoteLocalLiteMember() {
        MemberImpl member = getLocalMember();
        if (!member.isLiteMember()) {
            throw new IllegalStateException(member + " is not a lite member!");
        }

        MemberImpl master = getMasterMember();
        PromoteLiteMemberOp op = new PromoteLiteMemberOp();
        op.setCallerUuid(member.getUuid());

        InternalCompletableFuture<MembersView> future =
                nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME, op, master.getAddress());
        MembersView view = future.join();

        lock.lock();
        try {
            if (!member.getAddress().equals(master.getAddress())) {
                updateMembers(view, master.getAddress(), master.getUuid(), getThisUuid());
            }

            MemberImpl localMemberInMemberList = membershipManager.getMember(member.getAddress());
            if (localMemberInMemberList.isLiteMember()) {
                throw new IllegalStateException("Cannot promote to data member! Previous master was: " + master.getAddress()
                        + ", Current master is: " + getMasterAddress());
            }
        } finally {
            lock.unlock();
        }
    }

    MemberImpl promoteAndGetLocalMember() {
        MemberImpl member = getLocalMember();
        assert member.isLiteMember() : "Local member is not lite member!";
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";

        localMember = new MemberImpl(member.getAddress(), member.getVersion(), true, member.getUuid(),
                member.getAttributes(), false, member.getMemberListJoinVersion(), node.hazelcastInstance);
        node.loggingService.setThisMember(localMember);
        return localMember;
    }

    @Override
    public int getMemberListVersion() {
        return membershipManager.getMemberListVersion();
    }

    private MemberImpl getMasterMember() {
        MemberImpl master;
        lock.lock();
        try {
            Address masterAddress = getMasterAddress();
            if (masterAddress == null) {
                throw new IllegalStateException("Master is not known yet!");
            }

            master = getMember(masterAddress);
        } finally {
            lock.unlock();
        }
        return master;
    }

    @Override
    public String toString() {
        return "ClusterService" + "{address=" + getThisAddress() + '}';
    }
}
