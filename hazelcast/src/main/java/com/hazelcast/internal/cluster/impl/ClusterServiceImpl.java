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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberAttributeEvent;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.LifecycleServiceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.operations.ExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.cluster.impl.operations.PromoteLiteMemberOp;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerExplicitSuspicionOp;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MemberAttributeServiceEvent;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.version.Version;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.cluster.impl.MemberImpl.NA_MEMBER_LIST_JOIN_VERSION;
import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.SYSTEM_EXECUTOR;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.lang.String.format;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class ClusterServiceImpl implements ClusterService, ConnectionListener, ManagedService,
        EventPublishingService<MembershipEvent, MembershipListener>, TransactionalService {

    public static final String SERVICE_NAME = "hz:core:clusterService";
    public static final String SPLIT_BRAIN_HANDLER_EXECUTOR_NAME = "hz:cluster:splitbrain";

    static final String CLUSTER_EXECUTOR_NAME = "hz:cluster";
    static final String MEMBERSHIP_EVENT_EXECUTOR_NAME = "hz:cluster:event";
    static final String VERSION_AUTO_UPGRADE_EXECUTOR_NAME = "hz:cluster:version:auto:upgrade";

    private static final int DEFAULT_MERGE_RUN_DELAY_MILLIS = 100;
    private static final long CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS = 1000;
    private static final boolean ASSERTION_ENABLED = ClusterServiceImpl.class.desiredAssertionStatus();
    private static final String TRANSACTION_OPTIONS_MUST_NOT_BE_NULL = "Transaction options must not be null!";
    private static final String STATE_MUST_NOT_BE_NULL = "State must not be null!";
    private static final String VERSION_MUST_NOT_BE_NULL = "Version must not be null!";

    private final boolean useLegacyMemberListFormat;
    private final Node node;
    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final ClusterClockImpl clusterClock;
    private final MembershipManager membershipManager;
    private final ClusterJoinManager clusterJoinManager;
    private final ClusterStateManager clusterStateManager;
    private final ClusterHeartbeatManager clusterHeartbeatManager;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean joined = new AtomicBoolean(false);

    private volatile String clusterId;
    private volatile Address masterAddress;
    private volatile MemberImpl localMember;

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

        node.networkingService.getEndpointManager(MEMBER).addConnectionListener(this);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(CLUSTER_EXECUTOR_NAME, 2, Integer.MAX_VALUE, ExecutorType.CACHED);
        executionService.register(SPLIT_BRAIN_HANDLER_EXECUTOR_NAME, 2, Integer.MAX_VALUE, ExecutorType.CACHED);
        //MEMBERSHIP_EVENT_EXECUTOR is a single threaded executor to ensure that events are executed in correct order.
        executionService.register(MEMBERSHIP_EVENT_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
        executionService.register(VERSION_AUTO_UPGRADE_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
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
        long mergeFirstRunDelayMs = node.getProperties().getPositiveMillisOrDefault(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS,
                DEFAULT_MERGE_RUN_DELAY_MILLIS);
        long mergeNextRunDelayMs = node.getProperties().getPositiveMillisOrDefault(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS,
                DEFAULT_MERGE_RUN_DELAY_MILLIS);

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(SPLIT_BRAIN_HANDLER_EXECUTOR_NAME, new SplitBrainHandler(node),
                mergeFirstRunDelayMs, mergeNextRunDelayMs, TimeUnit.MILLISECONDS);

        membershipManager.init();
        clusterHeartbeatManager.init();
    }

    public void sendLocalMembershipEvent() {
        membershipManager.sendMembershipEvents(Collections.emptySet(), Collections.singleton(getLocalMember()));
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

            Connection conn = node.getEndpointManager(MEMBER).getConnection(address);
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

    public MembersView handleMastershipClaim(@Nonnull Address candidateAddress,
                                             @Nonnull String candidateUuid) {
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

        Address memberAddress = node.getThisAddress();
        Map<EndpointQualifier, Address> addressMap = localMember.getAddressMap();
        String newUuid = UuidUtil.createMemberUuid(memberAddress);

        logger.warning("Resetting local member UUID. Previous: " + localMember.getUuid() + ", new: " + newUuid);

        localMember = new MemberImpl.Builder(addressMap)
                .version(localMember.getVersion())
                .localMember(true)
                .uuid(newUuid)
                .attributes(localMember.getAttributes())
                .liteMember(localMember.isLiteMember())
                .memberListJoinVersion(localMember.getMemberListJoinVersion())
                .instance(node.hazelcastInstance)
                .build();

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

            try {
                initialClusterState(clusterState, clusterVersion);
            } catch (VersionMismatchException e) {
                // node should shutdown since it cannot handle the cluster version
                // it is safe to do so here because no operations have been executed yet
                logger.severe(format("This member will shutdown because it cannot join the cluster: %s", e.getMessage()));
                node.shutdown(true);
                return false;
            }
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

    public void updateMemberAttribute(String uuid, MemberAttributeOperationType operationType, String key, String value) {
        lock.lock();
        try {
            MemberMap memberMap = membershipManager.getMemberMap();
            MemberImpl member = memberMap.getMember(uuid);
            if (!member.equals(getLocalMember())) {
                member.updateAttribute(operationType, key, value);
            }
            sendMemberAttributeEvent(member, memberMap, operationType, key, value);
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

    /**
     * Returns whether member with given identity (either {@code UUID} or {@code Address}
     * depending on Hot Restart is enabled or not) is a known missing member or not.
     *
     * @param address Address of the missing member
     * @param uuid    Uuid of the missing member
     * @return true if it's a known missing member, false otherwise
     */
    public boolean isMissingMember(Address address, String uuid) {
        return membershipManager.isMissingMember(address, uuid);
    }

    public Collection<Member> getActiveAndMissingMembers() {
        return membershipManager.getActiveAndMissingMembers();
    }

    public void notifyForRemovedMember(MemberImpl member) {
        lock.lock();
        try {
            membershipManager.onMemberRemove(member);
        } finally {
            lock.unlock();
        }
    }

    public void shrinkMissingMembers(Collection<String> memberUuidsToRemove) {
        membershipManager.shrinkMissingMembers(memberUuidsToRemove);
    }

    private void sendMemberAttributeEvent(MemberImpl member, MemberMap memberMap, MemberAttributeOperationType operationType,
                                          String key, Object value) {
        Set<Member> members = new HashSet<>(memberMap.getMembers());
        final MemberAttributeServiceEvent event
                = new MemberAttributeServiceEvent(this, member, members, operationType, key, value);
        MemberAttributeEvent attributeEvent = new MemberAttributeEvent(this, member, members, operationType,
                key, value);
        Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        if (membershipAwareServices != null && !membershipAwareServices.isEmpty()) {
            for (final MembershipAwareService service : membershipAwareServices) {
                // service events should not block each other
                nodeEngine.getExecutionService().execute(SYSTEM_EXECUTOR, () -> service.memberAttributeChanged(event));
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
    public MemberImpl getMember(Address address, String uuid) {
        if (address == null || uuid == null) {
            return null;
        }
        return membershipManager.getMember(address, uuid);
    }

    @Override
    public Collection<MemberImpl> getMemberImpls() {
        return membershipManager.getMembers();
    }

    public Collection<Address> getMemberAddresses() {
        return membershipManager.getMemberMap().getAddresses();
    }

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
        if (logger.isFineEnabled()) {
            logger.fine("Setting master address to " + master);
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

    public String addMembershipListener(@Nonnull MembershipListener listener) {
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

    public boolean removeMembershipListener(@Nonnull String registrationId) {
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

    @Nonnull
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
    public void changeClusterState(@Nonnull ClusterState newState) {
        checkNotNull(newState, STATE_MUST_NOT_BE_NULL);
        changeClusterState(newState, false);
    }

    private void changeClusterState(ClusterState newState, boolean isTransient) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(newState), membershipManager.getMemberMap(),
                partitionStateVersion, isTransient);
    }

    @Override
    public void changeClusterState(@Nonnull ClusterState newState, @Nonnull TransactionOptions options) {
        checkNotNull(newState, STATE_MUST_NOT_BE_NULL);
        checkNotNull(options, TRANSACTION_OPTIONS_MUST_NOT_BE_NULL);
        changeClusterState(newState, options, false);
    }

    private void changeClusterState(@Nonnull ClusterState newState,
                                    @Nonnull TransactionOptions options,
                                    boolean isTransient) {
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
    public void changeClusterVersion(@Nonnull Version version) {
        checkNotNull(version, VERSION_MUST_NOT_BE_NULL);
        MemberMap memberMap = membershipManager.getMemberMap();
        changeClusterVersion(version, memberMap);
    }

    public void changeClusterVersion(@Nonnull Version version, @Nonnull MemberMap memberMap) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(version), memberMap, partitionStateVersion, false);
    }

    @Override
    public void changeClusterVersion(@Nonnull Version version,
                                     @Nonnull TransactionOptions options) {
        checkNotNull(version, VERSION_MUST_NOT_BE_NULL);
        checkNotNull(options, TRANSACTION_OPTIONS_MUST_NOT_BE_NULL);
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

    @Override
    public void shutdown() {
        shutdownCluster(null);
    }

    @Override
    public void shutdown(@Nullable TransactionOptions options) {
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

        if (node.config.getCPSubsystemConfig().getCPMemberCount() == 0) {
            shutdownNodesConcurrently(timeoutNanos);
        } else {
            shutdownNodesSerially(timeoutNanos);
        }
    }

    private void shutdownNodesConcurrently(final long timeoutNanos) {
        Operation op = new ShutdownNodeOp();
        Collection<Member> members = getMembers(NON_LOCAL_MEMBER_SELECTOR);
        long startTime = System.nanoTime();

        logger.info("Sending shut down operations to all members...");

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

        logger.info("Number of other members remaining: " + getSize(NON_LOCAL_MEMBER_SELECTOR) + ". Shutting down itself.");

        HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
        hazelcastInstance.getLifecycleService().shutdown();
    }

    private void shutdownNodesSerially(final long timeoutNanos) {
        Operation op = new ShutdownNodeOp();
        long startTime = System.nanoTime();
        Collection<Member> members = getMembers(NON_LOCAL_MEMBER_SELECTOR);

        logger.info("Sending shut down operations to other members one by one...");

        while ((System.nanoTime() - startTime) < timeoutNanos && !members.isEmpty()) {
            Member member = members.iterator().next();
            nodeEngine.getOperationService().send(op, member.getAddress());
            members = getMembers(NON_LOCAL_MEMBER_SELECTOR);

            try {
                Thread.sleep(CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("Shutdown sleep interrupted. ", e);
                break;
            }
        }

        logger.info("Number of other members remaining: " + getSize(NON_LOCAL_MEMBER_SELECTOR) + ". Shutting down itself.");

        HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
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

        localMember = new MemberImpl.Builder(member.getAddressMap())
                .version(member.getVersion())
                .localMember(true)
                .uuid(member.getUuid())
                .attributes(member.getAttributes())
                .memberListJoinVersion(member.getMemberListJoinVersion())
                .instance(node.hazelcastInstance)
                .build();
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
