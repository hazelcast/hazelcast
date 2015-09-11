/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.cluster.impl.operations.AuthenticationFailureOperation;
import com.hazelcast.cluster.impl.operations.BeforeJoinCheckFailureOperation;
import com.hazelcast.cluster.impl.operations.ConfigMismatchOperation;
import com.hazelcast.cluster.impl.operations.FinalizeJoinOperation;
import com.hazelcast.cluster.impl.operations.GroupMismatchOperation;
import com.hazelcast.cluster.impl.operations.HeartbeatOperation;
import com.hazelcast.cluster.impl.operations.JoinRequestOperation;
import com.hazelcast.cluster.impl.operations.MasterConfirmationOperation;
import com.hazelcast.cluster.impl.operations.MasterDiscoveryOperation;
import com.hazelcast.cluster.impl.operations.MemberInfoUpdateOperation;
import com.hazelcast.cluster.impl.operations.MemberRemoveOperation;
import com.hazelcast.cluster.impl.operations.PostJoinOperation;
import com.hazelcast.cluster.impl.operations.SetMasterOperation;
import com.hazelcast.cluster.impl.operations.TriggerMemberListPublishOperation;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;
import com.hazelcast.security.Credentials;
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
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.executor.ExecutorType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.ConnectException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.cluster.impl.operations.FinalizeJoinOperation.FINALIZE_JOIN_MAX_TIMEOUT;
import static com.hazelcast.cluster.impl.operations.FinalizeJoinOperation.FINALIZE_JOIN_TIMEOUT_FACTOR;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGING;
import static com.hazelcast.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public final class ClusterServiceImpl implements ClusterService, ConnectionListener, ManagedService,
        EventPublishingService<MembershipEvent, MembershipListener> {

    public static final String SERVICE_NAME = "hz:core:clusterService";

    private static final String EXECUTOR_NAME = "hz:cluster";

    private static final long HEARTBEAT_LOG_THRESHOLD = 10000L;
    private static final int HEART_BEAT_INTERVAL_FACTOR = 10;

    private static final int DEFAULT_MERGE_RUN_DELAY_MILLIS = 100;
    private static final int PREPARE_TO_MERGE_EXECUTION_DELAY_SECONDS = 10;
    private static final int MERGE_EXECUTOR_SERVICE_QUEUE_CAPACITY = 1000;

    private static final int CLUSTER_OPERATION_RETRY_COUNT = 100;
    private static final int MAX_PING_RETRY_COUNT = 5;

    private static final long MIN_WAIT_ON_FUTURE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private static final String MEMBERSHIP_EVENT_EXECUTOR_NAME = "hz:cluster:event";

    private final Address thisAddress;
    private final MemberImpl thisMember;

    private final Lock lock = new ReentrantLock();

    private final Set<MemberInfo> setJoins = new LinkedHashSet<MemberInfo>(100);

    private final AtomicReference<Map<Address, MemberImpl>> membersMapRef
            = new AtomicReference<Map<Address, MemberImpl>>(Collections.<Address, MemberImpl>emptyMap());

    private final AtomicReference<Set<MemberImpl>> membersRef
            = new AtomicReference<Set<MemberImpl>>(Collections.<MemberImpl>emptySet());

    private final AtomicBoolean preparingToMerge = new AtomicBoolean();

    private final ConcurrentMap<MemberImpl, Long> heartbeatTimes = new ConcurrentHashMap<MemberImpl, Long>();

    private final ConcurrentMap<MemberImpl, Long> masterConfirmationTimes = new ConcurrentHashMap<MemberImpl, Long>();

    private final Node node;
    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    private final long maxWaitMillisBeforeJoin;
    private final long waitMillisBeforeJoin;

    private final long maxNoHeartbeatMillis;
    private final long maxNoMasterConfirmationMillis;

    private final long heartbeatIntervalMillis;
    private final long pingIntervalMillis;

    private final boolean icmpEnabled;
    private final int icmpTtl;
    private final int icmpTimeoutMillis;

    private final ExceptionHandler whileFinalizeJoinsExceptionHandler;

    private final ClusterClockImpl clusterClock;

    private String clusterId;

    private long timeToStartJoin;

    private long firstJoinRequest;

    private volatile boolean joinInProgress;

    @Probe(name = "lastHeartBeat")
    private volatile long lastHeartBeat;

    public ClusterServiceImpl(Node node) {
        this.node = node;
        nodeEngine = node.nodeEngine;

        logger = node.getLogger(ClusterService.class.getName());
        clusterClock = new ClusterClockImpl(logger);
        whileFinalizeJoinsExceptionHandler = logAllExceptions(logger, "While waiting finalize join calls...", Level.WARNING);

        thisAddress = node.getThisAddress();
        thisMember = node.getLocalMember();

        registerMember(thisMember);

        maxWaitMillisBeforeJoin = node.groupProperties.getMillis(GroupProperty.MAX_WAIT_SECONDS_BEFORE_JOIN);
        waitMillisBeforeJoin = node.groupProperties.getMillis(GroupProperty.WAIT_SECONDS_BEFORE_JOIN);
        maxNoHeartbeatMillis = node.groupProperties.getMillis(GroupProperty.MAX_NO_HEARTBEAT_SECONDS);
        maxNoMasterConfirmationMillis = node.groupProperties.getMillis(GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS);

        heartbeatIntervalMillis = getHeartBeatInterval(node.groupProperties);
        pingIntervalMillis = heartbeatIntervalMillis * HEART_BEAT_INTERVAL_FACTOR;

        icmpEnabled = node.groupProperties.getBoolean(GroupProperty.ICMP_ENABLED);
        icmpTtl = node.groupProperties.getInteger(GroupProperty.ICMP_TTL);
        icmpTimeoutMillis = (int) node.groupProperties.getMillis(GroupProperty.ICMP_TIMEOUT);

        node.connectionManager.addConnectionListener(this);

        //MEMBERSHIP_EVENT_EXECUTOR is a single threaded executor to ensure that events are executed in correct order.
        nodeEngine.getExecutionService().register(MEMBERSHIP_EVENT_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);

        registerMetrics();
    }

    private static long getHeartBeatInterval(GroupProperties groupProperties) {
        long heartbeatInterval = groupProperties.getMillis(GroupProperty.HEARTBEAT_INTERVAL_SECONDS);
        return heartbeatInterval > 0 ? heartbeatInterval : TimeUnit.SECONDS.toMillis(1);
    }

    private void registerMember(MemberImpl thisMember) {
        setMembers(thisMember);
        sendMembershipEvents(Collections.<MemberImpl>emptySet(), Collections.singleton(thisMember));
    }

    private void registerMetrics() {
        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        metricsRegistry.scanAndRegister(clusterClock, "cluster.clock");
        metricsRegistry.scanAndRegister(this, "cluster");
    }

    @Override
    public ClusterClockImpl getClusterClock() {
        return clusterClock;
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
        long mergeFirstRunDelayMs = node.groupProperties.getMillis(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS);
        mergeFirstRunDelayMs = (mergeFirstRunDelayMs > 0 ? mergeFirstRunDelayMs : DEFAULT_MERGE_RUN_DELAY_MILLIS);

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(EXECUTOR_NAME, 2, MERGE_EXECUTOR_SERVICE_QUEUE_CAPACITY, ExecutorType.CACHED);

        long mergeNextRunDelayMs = node.groupProperties.getMillis(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS);
        mergeNextRunDelayMs = (mergeNextRunDelayMs > 0 ? mergeNextRunDelayMs : DEFAULT_MERGE_RUN_DELAY_MILLIS);
        executionService.scheduleWithFixedDelay(EXECUTOR_NAME, new SplitBrainHandler(node),
                mergeFirstRunDelayMs, mergeNextRunDelayMs, TimeUnit.MILLISECONDS);

        executionService.scheduleWithFixedDelay(EXECUTOR_NAME, new Runnable() {
            public void run() {
                heartBeater();
            }
        }, heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);

        long masterConfirmationInterval = node.groupProperties.getSeconds(GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS);
        masterConfirmationInterval = (masterConfirmationInterval > 0 ? masterConfirmationInterval : 1);
        executionService.scheduleWithFixedDelay(EXECUTOR_NAME, new Runnable() {
            public void run() {
                sendMasterConfirmation();
            }
        }, masterConfirmationInterval, masterConfirmationInterval, TimeUnit.SECONDS);

        long memberListPublishInterval = node.groupProperties.getSeconds(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS);
        memberListPublishInterval = (memberListPublishInterval > 0 ? memberListPublishInterval : 1);
        executionService.scheduleWithFixedDelay(EXECUTOR_NAME, new Runnable() {
            public void run() {
                sendMemberListToOthers();
            }
        }, memberListPublishInterval, memberListPublishInterval, TimeUnit.SECONDS);
    }

    public boolean isJoinInProgress() {
        if (joinInProgress) {
            return true;
        }

        lock.lock();
        try {
            return joinInProgress || !setJoins.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    public boolean validateJoinMessage(JoinMessage joinMessage) throws Exception {
        if (joinMessage.getPacketVersion() != Packet.VERSION) {
            return false;
        }
        try {
            ConfigCheck newMemberConfigCheck = joinMessage.getConfigCheck();
            ConfigCheck clusterConfigCheck = node.createConfigCheck();
            return clusterConfigCheck.isCompatible(newMemberConfigCheck);
        } catch (Exception e) {
            logger.warning(format("Invalid join request from %s, cause: %s", joinMessage.getAddress(), e.getMessage()));
            throw e;
        }
    }

    private boolean isValidJoinMessage(JoinMessage joinMessage) {
        try {
            return validateJoinMessage(joinMessage);
        } catch (ConfigMismatchException e) {
            throw e;
        } catch (Exception e) {
            return false;
        }
    }

    private void logIfConnectionToEndpointIsMissing(long now, MemberImpl member) {
        long heartbeatTime = getHeartbeatTime(member);
        if ((now - heartbeatTime) >= pingIntervalMillis) {
            Connection conn = node.connectionManager.getOrConnect(member.getAddress());
            if (conn == null || !conn.isAlive()) {
                logger.warning("This node does not have a connection to " + member);
            }
        }
    }

    private long getHeartbeatTime(MemberImpl member) {
        Long heartbeatTime = heartbeatTimes.get(member);
        return (heartbeatTime != null ? heartbeatTime : 0L);
    }

    private void heartBeater() {
        if (!node.joined() || !node.isActive()) {
            return;
        }

        long now = Clock.currentTimeMillis();

        // compensate for any abrupt jumps forward in the system clock
        long clockJump = 0L;
        if (lastHeartBeat != 0L) {
            clockJump = now - lastHeartBeat - heartbeatIntervalMillis;
            if (Math.abs(clockJump) > HEARTBEAT_LOG_THRESHOLD) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                logger.info(format("System clock apparently jumped from %s to %s since last heartbeat (%+d ms)",
                        sdf.format(new Date(lastHeartBeat)), sdf.format(new Date(now)), clockJump));
            }
            clockJump = Math.max(0L, clockJump);

            if (clockJump >= maxNoMasterConfirmationMillis / 2) {
                logger.warning(format("Resetting master confirmation timestamps because of huge system clock jump!"
                        + " Clock-Jump: %d ms, Master-Confirmation-Timeout: %d ms", clockJump, maxNoMasterConfirmationMillis));
                resetMemberMasterConfirmations();
            }
        }
        lastHeartBeat = now;

        if (node.isMaster()) {
            heartBeaterMaster(now, clockJump);
        } else {
            heartBeaterSlave(now, clockJump);
        }
    }

    private void heartBeaterMaster(long now, long clockJump) {
        Collection<MemberImpl> members = getMemberImpls();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                try {
                    logIfConnectionToEndpointIsMissing(now, member);
                    if (removeMemberIfNotHeartBeating(now - clockJump, member)) {
                        continue;
                    }

                    if (removeMemberIfMasterConfirmationExpired(now - clockJump, member)) {
                        continue;
                    }

                    pingMemberIfRequired(now, member);
                    sendHeartbeat(member.getAddress());
                } catch (Throwable e) {
                    logger.severe(e);
                }
            }
        }
    }

    private boolean removeMemberIfNotHeartBeating(long now, MemberImpl member) {
        long heartbeatTime = getHeartbeatTime(member);
        if ((now - heartbeatTime) > maxNoHeartbeatMillis) {
            logger.warning(format("Removing %s because it has not sent any heartbeats for %d ms."
                    + " Last heartbeat time was %s", member, maxNoHeartbeatMillis, new Date(heartbeatTime)));
            removeAddress(member.getAddress());
            return true;
        }
        if (logger.isFinestEnabled() && (now - heartbeatTime) > heartbeatIntervalMillis * HEART_BEAT_INTERVAL_FACTOR) {
            logger.finest(format("Not receiving any heartbeats from %s since %s", member, new Date(heartbeatTime)));
        }
        return false;
    }

    private boolean removeMemberIfMasterConfirmationExpired(long now, MemberImpl member) {
        Long lastConfirmation = masterConfirmationTimes.get(member);
        if (lastConfirmation == null) {
            lastConfirmation = 0L;
        }
        if (now - lastConfirmation > maxNoMasterConfirmationMillis) {
            logger.warning(format("Removing %s because it has not sent any master confirmation for %d ms. "
                    + " Last confirmation time was %s", member, maxNoMasterConfirmationMillis, new Date(lastConfirmation)));
            removeAddress(member.getAddress());
            return true;
        }
        return false;
    }

    private void heartBeaterSlave(long now, long clockJump) {
        Collection<MemberImpl> members = getMemberImpls();

        for (MemberImpl member : members) {
            if (!member.localMember()) {
                try {
                    logIfConnectionToEndpointIsMissing(now, member);

                    if (isMaster(member)) {
                        if (removeMemberIfNotHeartBeating(now - clockJump, member)) {
                            continue;
                        }
                    }

                    pingMemberIfRequired(now, member);
                    sendHeartbeat(member.getAddress());
                } catch (Throwable e) {
                    logger.severe(e);
                }
            }
        }
    }

    private boolean isMaster(MemberImpl member) {
        return member.getAddress().equals(getMasterAddress());
    }

    private void pingMemberIfRequired(long now, MemberImpl member) {
        if (!icmpEnabled) {
            return;
        }
        if ((now - getHeartbeatTime(member)) >= pingIntervalMillis) {
            ping(member);
        }
    }

    private void ping(final MemberImpl memberImpl) {
        nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
            public void run() {
                try {
                    Address address = memberImpl.getAddress();
                    logger.warning(format("%s will ping %s", thisAddress, address));
                    for (int i = 0; i < MAX_PING_RETRY_COUNT; i++) {
                        try {
                            if (address.getInetAddress().isReachable(null, icmpTtl, icmpTimeoutMillis)) {
                                logger.info(format("%s pinged %s successfully", thisAddress, address));
                                return;
                            }
                        } catch (ConnectException ignored) {
                            // no route to host, means we cannot connect anymore
                            EmptyStatement.ignore(ignored);
                        }
                    }
                    // host not reachable
                    logger.warning(format("%s could not ping %s", thisAddress, address));
                    removeAddress(address);
                } catch (Throwable ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
        });
    }

    private void sendHeartbeat(Address target) {
        if (target == null) {
            return;
        }
        try {
            node.nodeEngine.getOperationService().send(new HeartbeatOperation(clusterClock.getClusterTime()), target);
        } catch (Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest(format("Error while sending heartbeat -> %s[%s]", e.getClass().getName(), e.getMessage()));
            }
        }
    }

    public void sendMasterConfirmation() {
        if (!node.joined() || !node.isActive() || isMaster()) {
            return;
        }
        Address masterAddress = getMasterAddress();
        if (masterAddress == null) {
            logger.finest("Could not send MasterConfirmation, masterAddress is null!");
            return;
        }
        MemberImpl masterMember = getMember(masterAddress);
        if (masterMember == null) {
            logger.finest("Could not send MasterConfirmation, masterMember is null!");
            return;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Sending MasterConfirmation to " + masterMember);
        }
        nodeEngine.getOperationService().send(new MasterConfirmationOperation(clusterClock.getClusterTime()), masterAddress);
    }

    // will be called just before this node becomes the master
    private void resetMemberMasterConfirmations() {
        long now = Clock.currentTimeMillis();
        for (MemberImpl member : getMemberImpls()) {
            masterConfirmationTimes.put(member, now);
        }
    }

    public void sendMemberListToMember(Address target) {
        if (!isMaster()) {
            return;
        }
        if (thisAddress.equals(target)) {
            return;
        }
        Collection<MemberImpl> members = getMemberImpls();
        MemberInfoUpdateOperation op = new MemberInfoUpdateOperation(
                createMemberInfoList(members), clusterClock.getClusterTime(), false);
        nodeEngine.getOperationService().send(op, target);
    }

    private void sendMemberListToOthers() {
        if (!isMaster()) {
            return;
        }
        Collection<MemberImpl> members = getMemberImpls();
        MemberInfoUpdateOperation op = new MemberInfoUpdateOperation(
                createMemberInfoList(members), clusterClock.getClusterTime(), false);
        for (MemberImpl member : members) {
            if (member.equals(thisMember)) {
                continue;
            }
            nodeEngine.getOperationService().send(op, member.getAddress());
        }
    }

    public void removeAddress(Address deadAddress) {
        doRemoveAddress(deadAddress, true);
    }

    private void doRemoveAddress(Address deadAddress, boolean destroyConnection) {
        if (!ensureMemberIsRemovable(deadAddress)) {
            return;
        }

        lock.lock();
        try {
            if (deadAddress.equals(node.getMasterAddress())) {
                assignNewMaster();
            }
            if (node.isMaster()) {
                setJoins.remove(new MemberInfo(deadAddress));
            }
            Connection conn = node.connectionManager.getConnection(deadAddress);
            if (destroyConnection && conn != null) {
                node.connectionManager.destroyConnection(conn);
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
        if (preparingToMerge.get()) {
            logger.warning("Cluster-merge process is ongoing, won't process member removal " + deadAddress);
            return false;
        }
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

        if (logger.isFinestEnabled()) {
            logger.finest(format("Old master: %s, new master: %s ", oldMasterAddress, node.getMasterAddress()));
        }

        if (node.isMaster()) {
            resetMemberMasterConfirmations();
            clusterClock.reset();
        } else {
            sendMasterConfirmation();
        }
    }

    public void answerMasterQuestion(JoinMessage joinMessage, Connection connection) {
        if (!ensureValidConfiguration(joinMessage)) {
            return;
        }

        if (node.getMasterAddress() != null) {
            if (!checkIfJoinRequestFromAnExistingMember(joinMessage, connection)) {
                sendMasterAnswer(joinMessage.getAddress());
            }
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest(format("Received a master question from %s,"
                        + " but this node is not master itself or doesn't have a master yet!", joinMessage.getAddress()));
            }
        }
    }

    public void handleJoinRequest(JoinRequest joinRequest, Connection connection) {
        if (!ensureNodeIsReady()) {
            return;
        }
        if (!ensureValidConfiguration(joinRequest)) {
            return;
        }

        Address target = joinRequest.getAddress();
        boolean isRequestFromCurrentMaster = target.equals(node.getMasterAddress());
        // if the join request from current master, do not send a master answer,
        // because master can somehow dropped its connection and wants to join back
        if (!node.isMaster() && !isRequestFromCurrentMaster) {
            sendMasterAnswer(target);
            return;
        }

        if (joinInProgress) {
            if (logger.isFinestEnabled()) {
                logger.finest(format("Join is in progress, cannot handle join request from %s at the moment", target));
            }
            return;
        }

        executeJoinRequest(joinRequest, connection, target);
    }

    private boolean ensureNodeIsReady() {
        if (node.joined() && node.isActive()) {
            return true;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Node is not ready to process join request...");
        }
        return false;
    }

    private boolean ensureValidConfiguration(JoinMessage joinMessage) {
        Address address = joinMessage.getAddress();
        try {
            if (isValidJoinMessage(joinMessage)) {
                return true;
            }

            logger.warning(format("Received an invalid join request from %s, cause: clusters part of different cluster-groups",
                    address));
            nodeEngine.getOperationService().send(new GroupMismatchOperation(), address);
        } catch (ConfigMismatchException e) {
            logger.warning(format("Received an invalid join request from %s, cause: %s", address, e.getMessage()));
            OperationService operationService = nodeEngine.getOperationService();
            operationService.send(new ConfigMismatchOperation(e.getMessage()), address);
        }
        return false;
    }

    private void executeJoinRequest(JoinRequest joinRequest, Connection connection, Address target) {
        lock.lock();
        try {
            if (checkIfJoinRequestFromAnExistingMember(joinRequest, connection)) {
                return;
            }

            long now = Clock.currentTimeMillis();
            if (logger.isFinestEnabled()) {
                String timeToStart = (timeToStartJoin > 0 ? ", timeToStart: " + (timeToStartJoin - now) : "");
                logger.finest(format("Handling join from %s, joinInProgress: %b%s", target, joinInProgress, timeToStart));
            }

            MemberInfo memberInfo = getMemberInfo(joinRequest, target);
            if (memberInfo == null) {
                return;
            }

            if (!prepareJoinIfNodeIsMaster(target)) {
                return;
            }

            startJoinRequest(target, now, memberInfo);
        } finally {
            lock.unlock();
        }
    }

    private MemberInfo getMemberInfo(JoinRequest joinRequest, Address target) {
        MemberInfo memberInfo = new MemberInfo(target, joinRequest.getUuid(), joinRequest.getAttributes());
        if (!setJoins.contains(memberInfo)) {
            try {
                checkSecureLogin(joinRequest, memberInfo);
            } catch (Exception e) {
                ILogger securityLogger = node.loggingService.getLogger("com.hazelcast.security");
                nodeEngine.getOperationService().send(new AuthenticationFailureOperation(), target);
                securityLogger.severe(e);
                return null;
            }
        }
        return memberInfo;
    }

    private void checkSecureLogin(JoinRequest joinRequest, MemberInfo newMemberInfo) {
        if (node.securityContext != null && !setJoins.contains(newMemberInfo)) {
            Credentials credentials = joinRequest.getCredentials();
            if (credentials == null) {
                throw new SecurityException("Expecting security credentials, but credentials could not be found in join request");
            }
            try {
                LoginContext loginContext = node.securityContext.createMemberLoginContext(credentials);
                loginContext.login();
            } catch (LoginException e) {
                throw new SecurityException(format("Authentication has failed for %s@%s, cause: %s",
                        credentials.getPrincipal(), credentials.getEndpoint(), e.getMessage()));
            }
        }
    }

    private boolean prepareJoinIfNodeIsMaster(Address target) {
        if (node.isMaster()) {
            try {
                node.getNodeExtension().beforeJoin();
            } catch (Exception e) {
                logger.warning(e.getMessage());
                nodeEngine.getOperationService().send(new BeforeJoinCheckFailureOperation(e.getMessage()), target);
                return false;
            }
        }
        return true;
    }

    private void startJoinRequest(Address target, long now, MemberInfo memberInfo) {
        if (firstJoinRequest == 0) {
            firstJoinRequest = now;
        }

        if (setJoins.add(memberInfo)) {
            sendMasterAnswer(target);
            if (now - firstJoinRequest < maxWaitMillisBeforeJoin) {
                timeToStartJoin = now + waitMillisBeforeJoin;
            }
        }
        if (now > timeToStartJoin) {
            startJoin();
        }
    }

    private boolean checkIfJoinRequestFromAnExistingMember(JoinMessage joinMessage, Connection connection) {
        MemberImpl member = getMember(joinMessage.getAddress());
        if (member == null) {
            return false;
        }

        Address target = member.getAddress();
        if (joinMessage.getUuid().equals(member.getUuid())) {
            if (node.isMaster()) {
                if (logger.isFinestEnabled()) {
                    logger.finest(format("Ignoring join request, member already exists: %s", joinMessage));
                }

                // send members update back to node trying to join again...
                Operation[] postJoinOps = nodeEngine.getPostJoinOperations();
                boolean isPostJoinOperation = postJoinOps != null && postJoinOps.length > 0;
                PostJoinOperation postJoinOp = isPostJoinOperation ? new PostJoinOperation(postJoinOps) : null;
                Operation operation = new FinalizeJoinOperation(createMemberInfoList(getMemberImpls()), postJoinOp,
                        clusterClock.getClusterTime(), false);
                nodeEngine.getOperationService().send(operation, target);
            } else {
                sendMasterAnswer(target);
            }
            return true;
        }

        // remove old member and process join request:
        // - if this node is master OR
        // - if requesting address is equal to master node's address, that means the master node somehow disconnected
        //   and wants to join back, so drop old member and process join request if this node becomes master
        if (node.isMaster() || target.equals(node.getMasterAddress())) {
            logger.warning(format("New join request has been received from an existing endpoint %s."
                    + " Removing old member and processing join request...", member));

            doRemoveAddress(target, false);
            Connection existing = node.connectionManager.getConnection(target);
            if (existing != connection) {
                node.connectionManager.destroyConnection(existing);
                node.connectionManager.registerConnection(target, connection);
            }
            return false;
        }
        return true;
    }

    private void sendMasterAnswer(Address target) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            logger.info(format("Cannot send master answer to %s since master node is not known yet", target));
            return;
        }
        SetMasterOperation op = new SetMasterOperation(masterAddress);
        nodeEngine.getOperationService().send(op, target);
    }

    public void handleMaster(Address masterAddress, Address callerAddress) {
        if (node.joined()) {
            if (logger.isFinestEnabled()) {
                logger.finest(format("Ignoring master response %s from %s, this node is already joined",
                        masterAddress, callerAddress));
            }
            return;
        }

        if (node.getThisAddress().equals(masterAddress)) {
            if (node.isMaster()) {
                logger.finest(format("Ignoring master response %s from %s, this node is already master",
                        masterAddress, callerAddress));
            } else {
                node.setAsMaster();
            }
            return;
        }

        handleMasterResponse(masterAddress, callerAddress);
    }

    private void handleMasterResponse(Address masterAddress, Address callerAddress) {
        lock.lock();
        try {
            if (logger.isFinestEnabled()) {
                logger.finest(format("Handling master response %s from %s", masterAddress, callerAddress));
            }

            Address currentMaster = node.getMasterAddress();
            if (currentMaster == null || currentMaster.equals(masterAddress)) {
                setMasterAndJoin(masterAddress);
                return;
            }
            if (currentMaster.equals(callerAddress)) {
                logger.info(format("Setting master to %s since %s says it is not master anymore", masterAddress, currentMaster));
                setMasterAndJoin(masterAddress);
                return;
            }

            Connection conn = node.connectionManager.getConnection(currentMaster);
            if (conn != null && conn.isAlive()) {
                logger.info(format("Ignoring master response %s from %s since this node has an active master %s",
                        masterAddress, callerAddress, currentMaster));
                sendJoinRequest(currentMaster, true);
            } else {
                logger.warning(format("Ambiguous master response: This node has a master %s, but does not have a connection"
                                + " to %s. Sent master response as %s. Master field will be unset now...",
                        currentMaster, callerAddress, masterAddress));
                node.setMasterAddress(null);
            }
        } finally {
            lock.unlock();
        }
    }

    private void setMasterAndJoin(Address masterAddress) {
        node.setMasterAddress(masterAddress);
        node.connectionManager.getOrConnect(masterAddress);
        if (!sendJoinRequest(masterAddress, true)) {
            logger.warning("Could not create connection to possible master " + masterAddress);
        }
    }

    public void acceptMasterConfirmation(MemberImpl member, long timestamp) {
        if (member != null) {
            if (logger.isFinestEnabled()) {
                logger.finest("MasterConfirmation has been received from " + member);
            }
            long clusterTime = clusterClock.getClusterTime();
            if (clusterTime - timestamp > maxNoMasterConfirmationMillis / 2) {
                logger.warning(format("Ignoring master confirmation from %s, since it is expired (now: %s, timestamp: %s)",
                        member, new Date(clusterTime), new Date(timestamp)));
                return;
            }
            masterConfirmationTimes.put(member, Clock.currentTimeMillis());
        }
    }

    public void prepareToMerge(final Address newTargetAddress) {
        preparingToMerge.set(true);
        node.getJoiner().setTargetAddress(newTargetAddress);
        nodeEngine.getExecutionService().schedule(new Runnable() {
            public void run() {
                merge(newTargetAddress);
            }
        }, PREPARE_TO_MERGE_EXECUTION_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    public void merge(Address newTargetAddress) {
        if (preparingToMerge.compareAndSet(true, false)) {
            node.getJoiner().setTargetAddress(newTargetAddress);
            LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
            lifecycleService.runUnderLifecycleLock(new MergeTask());
        }
    }

    private void joinReset() {
        lock.lock();
        try {
            joinInProgress = false;
            setJoins.clear();
            timeToStartJoin = Clock.currentTimeMillis() + waitMillisBeforeJoin;
            firstJoinRequest = 0;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            joinInProgress = false;
            setJoins.clear();
            timeToStartJoin = 0;
            setMembersRef(Collections.singletonMap(thisAddress, thisMember));
            masterConfirmationTimes.clear();
        } finally {
            lock.unlock();
        }
    }

    private void startJoin() {
        logger.finest("Starting join...");
        lock.lock();
        try {
            try {
                joinInProgress = true;

                // pause migrations until join, member-update and post-join operations are completed
                node.getPartitionService().pauseMigration();
                Collection<MemberImpl> members = getMemberImpls();
                Collection<MemberInfo> memberInfos = createMemberInfoList(members);
                for (MemberInfo memberJoining : setJoins) {
                    memberInfos.add(memberJoining);
                }
                long time = clusterClock.getClusterTime();

                // post join operations must be lock free, that means no locks at all:
                // no partition locks, no key-based locks, no service level locks!
                Operation[] postJoinOps = nodeEngine.getPostJoinOperations();
                boolean createPostJoinOperation = (postJoinOps != null && postJoinOps.length > 0);
                PostJoinOperation postJoinOp = (createPostJoinOperation ? new PostJoinOperation(postJoinOps) : null);

                int count = members.size() - 1 + setJoins.size();
                List<Future> calls = new ArrayList<Future>(count);
                for (MemberInfo member : setJoins) {
                    long startTime = clusterClock.getClusterStartTime();
                    Operation joinOperation = new FinalizeJoinOperation(memberInfos, postJoinOp, time, clusterId, startTime);
                    calls.add(invokeClusterOperation(joinOperation, member.getAddress()));
                }
                for (MemberImpl member : members) {
                    if (!member.getAddress().equals(thisAddress)) {
                        Operation infoUpdateOperation = new MemberInfoUpdateOperation(memberInfos, time, true);
                        calls.add(invokeClusterOperation(infoUpdateOperation, member.getAddress()));
                    }
                }

                updateMembers(memberInfos);
                int timeout = Math.min(calls.size() * FINALIZE_JOIN_TIMEOUT_FACTOR, FINALIZE_JOIN_MAX_TIMEOUT);
                waitWithDeadline(calls, timeout, TimeUnit.SECONDS, whileFinalizeJoinsExceptionHandler);
            } finally {
                node.getPartitionService().resumeMigration();
            }
        } finally {
            lock.unlock();
        }
    }

    private static List<MemberInfo> createMemberInfoList(Collection<MemberImpl> members) {
        List<MemberInfo> memberInfos = new LinkedList<MemberInfo>();
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member));
        }
        return memberInfos;
    }

    private static Set<MemberInfo> createMemberInfoSet(Collection<MemberImpl> members) {
        Set<MemberInfo> memberInfos = new HashSet<MemberInfo>();
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member));
        }
        return memberInfos;
    }

    public void updateMembers(Collection<MemberInfo> members) {
        lock.lock();
        try {
            Map<Address, MemberImpl> currentMemberMap = membersMapRef.get();

            if (!shouldProcessMemberUpdate(currentMemberMap, members)) {
                return;
            }

            String scopeId = thisAddress.getScopeId();
            Collection<MemberImpl> newMembers = new LinkedList<MemberImpl>();
            MemberImpl[] updatedMembers = new MemberImpl[members.size()];
            int memberIndex = 0;
            for (MemberInfo memberInfo : members) {
                MemberImpl member = currentMemberMap.get(memberInfo.getAddress());
                if (member == null) {
                    member = createMember(memberInfo.getAddress(), memberInfo.getUuid(), scopeId, memberInfo.getAttributes());
                    newMembers.add(member);
                    long now = Clock.currentTimeMillis();
                    onHeartbeat(member, now);
                    masterConfirmationTimes.put(member, now);
                }
                updatedMembers[memberIndex++] = member;
            }

            setMembers(updatedMembers);
            sendMembershipEvents(currentMemberMap.values(), newMembers);

            joinReset();
            heartBeater();
            node.setJoined();
            logger.info(membersString());
        } finally {
            lock.unlock();
        }
    }

    private boolean shouldProcessMemberUpdate(Map<Address, MemberImpl> currentMembers,
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
            Set<MemberInfo> currentMemberInfos = createMemberInfoSet(currentMembers.values());
            if (currentMemberInfos.containsAll(newMemberInfos)) {
                logger.finest("Received a periodic member update, no need to process...");
            } else {
                logger.warning("Received an inconsistent member update "
                        + "which contains new members and removes some of the current members! "
                        + "Ignoring and requesting a new member update...");
                nodeEngine.getOperationService().send(new TriggerMemberListPublishOperation(), getMasterAddress());
            }
            return false;
        }

        Set<MemberInfo> currentMemberInfos = createMemberInfoSet(currentMembers.values());
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
            Map<Address, MemberImpl> memberMap = membersMapRef.get();
            for (MemberImpl member : memberMap.values()) {
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

    public boolean sendJoinRequest(Address toAddress, boolean withCredentials) {
        if (toAddress == null) {
            toAddress = node.getMasterAddress();
        }
        JoinRequestOperation joinRequest = new JoinRequestOperation(node.createJoinRequest(withCredentials));
        return nodeEngine.getOperationService().send(joinRequest, toAddress);
    }

    public boolean sendMasterQuestion(Address toAddress) {
        checkNotNull(toAddress, "No endpoint is specified!");

        BuildInfo buildInfo = node.getBuildInfo();
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildInfo.getBuildNumber(), thisAddress,
                thisMember.getUuid(), node.createConfigCheck());
        return nodeEngine.getOperationService().send(new MasterDiscoveryOperation(joinMessage), toAddress);
    }

    @Override
    public void connectionAdded(Connection connection) {
    }

    public void onHeartbeat(MemberImpl member, long timestamp) {
        if (member != null) {
            long clusterTime = clusterClock.getClusterTime();
            if (clusterTime - timestamp > maxNoHeartbeatMillis / 2) {
                logger.warning(format("Ignoring heartbeat from %s since it is expired (now: %s, timestamp: %s)",
                        member, new Date(clusterTime), new Date(timestamp)));
                return;
            }
            if (isMaster(member)) {
                clusterClock.setMasterTime(timestamp);
            }
            heartbeatTimes.put(member, timestamp);
        }
    }

    @Override
    public void connectionRemoved(Connection connection) {
        if (logger.isFinestEnabled()) {
            logger.finest("Removed connection " + connection.getEndPoint());
        }
        if (!node.joined()) {
            Address masterAddress = node.getMasterAddress();
            if (masterAddress != null && masterAddress.equals(connection.getEndPoint())) {
                node.setMasterAddress(null);
            }
        }
    }

    private Future invokeClusterOperation(Operation op, Address target) {
        return nodeEngine.getOperationService()
                .createInvocationBuilder(SERVICE_NAME, op, target)
                .setTryCount(CLUSTER_OPERATION_RETRY_COUNT).invoke();
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    private void setMembers(MemberImpl... members) {
        if (members == null || members.length == 0) {
            return;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Updating members " + Arrays.toString(members));
        }
        lock.lock();
        try {
            // !!! ORDERED !!!
            Map<Address, MemberImpl> memberMap = new LinkedHashMap<Address, MemberImpl>();
            for (MemberImpl member : members) {
                memberMap.put(member.getAddress(), member);
            }
            setMembersRef(memberMap);
        } finally {
            lock.unlock();
        }
    }

    private void removeMember(MemberImpl deadMember) {
        logger.info("Removing " + deadMember);
        lock.lock();
        try {
            Map<Address, MemberImpl> members = membersMapRef.get();
            if (members.containsKey(deadMember.getAddress())) {
                // !!! ORDERED !!!
                Map<Address, MemberImpl> newMembers = new LinkedHashMap<Address, MemberImpl>(members);
                newMembers.remove(deadMember.getAddress());
                masterConfirmationTimes.remove(deadMember);
                setMembersRef(newMembers);
                // sync call
                node.getPartitionService().memberRemoved(deadMember);
                // sync call
                nodeEngine.onMemberLeft(deadMember);
                if (node.isMaster()) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(deadMember + " is dead, sending remove to all other members...");
                    }
                    invokeMemberRemoveOperation(deadMember.getAddress());
                }
                // async events
                sendMembershipEventNotifications(deadMember,
                        unmodifiableSet(new LinkedHashSet<Member>(newMembers.values())), false);
            }
        } finally {
            lock.unlock();
        }
    }

    private void invokeMemberRemoveOperation(Address deadAddress) {
        for (Member member : getMembers()) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address) && !address.equals(deadAddress)) {
                nodeEngine.getOperationService().send(new MemberRemoveOperation(deadAddress), address);
            }
        }
    }

    public void sendShutdownMessage() {
        invokeMemberRemoveOperation(thisAddress);
    }

    private void sendMembershipEventNotifications(MemberImpl member, Set<Member> members, final boolean added) {
        int eventType = added ? MembershipEvent.MEMBER_ADDED : MembershipEvent.MEMBER_REMOVED;
        MembershipEvent membershipEvent = new MembershipEvent(getClusterProxy(), member, eventType, members);
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
                = new MemberAttributeServiceEvent(getClusterProxy(), member, operationType, key, value);
        MemberAttributeEvent attributeEvent = new MemberAttributeEvent(getClusterProxy(), member, operationType, key, value);
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

    private MemberImpl createMember(Address address, String nodeUuid, String ipV6ScopeId, Map<String, Object> attributes) {
        address.setScopeId(ipV6ScopeId);
        return new MemberImpl(address, thisAddress.equals(address), nodeUuid,
                (HazelcastInstanceImpl) nodeEngine.getHazelcastInstance(), attributes);
    }

    @Override
    public MemberImpl getMember(Address address) {
        if (address == null) {
            return null;
        }
        Map<Address, MemberImpl> memberMap = membersMapRef.get();
        return memberMap.get(address);
    }

    @Override
    public MemberImpl getMember(String uuid) {
        if (uuid == null) {
            return null;
        }

        Map<Address, MemberImpl> memberMap = membersMapRef.get();
        for (MemberImpl member : memberMap.values()) {
            if (uuid.equals(member.getUuid())) {
                return member;
            }
        }
        return null;
    }

    private void setMembersRef(Map<Address, MemberImpl> memberMap) {
        memberMap = unmodifiableMap(memberMap);
        // make values(), keySet() and entrySet() to be cached
        memberMap.values();
        memberMap.keySet();
        memberMap.entrySet();
        membersMapRef.set(memberMap);
        membersRef.set(unmodifiableSet(new LinkedHashSet<MemberImpl>(memberMap.values())));
    }

    @Override
    public Collection<MemberImpl> getMemberImpls() {
        return membersRef.get();
    }

    public Collection<Address> getMemberAddresses() {
        Map<Address, MemberImpl> map = membersMapRef.get();
        return map.keySet();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Member> getMembers() {
        return (Set) membersRef.get();
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
        Collection<MemberImpl> members = getMemberImpls();
        return (members != null ? members.size() : 0);
    }

    public String addMembershipListener(MembershipListener listener) {
        checkNotNull(listener, "listener cannot be null");

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration;
        if (listener instanceof InitialMembershipListener) {
            lock.lock();
            try {
                ((InitialMembershipListener) listener).init(new InitialMembershipEvent(getClusterProxy(), getMembers()));
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

    public Cluster getClusterProxy() {
        return new ClusterProxy(this);
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
    public String toString() {
        return "ClusterService"
                + "{address=" + thisAddress
                + '}';
    }

    private class MergeTask implements Runnable {

        public void run() {
            LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
            lifecycleService.fireLifecycleEvent(MERGING);

            resetState();

            Collection<Runnable> tasks = collectMergeTasks();

            resetServices();

            rejoin();

            executeMergeTasks(tasks);

            if (node.isActive() && node.joined()) {
                lifecycleService.fireLifecycleEvent(MERGED);
            }
        }

        private void resetState() {
            // reset node and membership state from now on this node won't be joined and won't have a master address
            node.reset();
            ClusterServiceImpl.this.reset();
            // stop the connection-manager:
            // - all socket connections will be closed
            // - connection listening thread will stop
            // - no new connection will be established
            node.connectionManager.stop();

            // clear waiting operations in queue and notify invocations to retry
            nodeEngine.reset();
        }

        private Collection<Runnable> collectMergeTasks() {
            // gather merge tasks from services
            Collection<SplitBrainHandlerService> services = nodeEngine.getServices(SplitBrainHandlerService.class);
            Collection<Runnable> tasks = new LinkedList<Runnable>();
            for (SplitBrainHandlerService service : services) {
                Runnable runnable = service.prepareMergeRunnable();
                if (runnable != null) {
                    tasks.add(runnable);
                }
            }
            return tasks;
        }

        private void resetServices() {
            // reset all services to their initial state
            Collection<ManagedService> managedServices = nodeEngine.getServices(ManagedService.class);
            for (ManagedService service : managedServices) {
                service.reset();
            }
        }

        private void rejoin() {
            // start connection-manager to setup and accept new connections
            node.connectionManager.start();
            // re-join to the target cluster
            node.rejoin();
        }

        private void executeMergeTasks(Collection<Runnable> tasks) {
            // execute merge tasks
            Collection<Future> futures = new LinkedList<Future>();
            for (Runnable task : tasks) {
                Future f = nodeEngine.getExecutionService().submit("hz:system", task);
                futures.add(f);
            }
            long callTimeoutMillis = node.groupProperties.getMillis(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS);
            for (Future f : futures) {
                try {
                    waitOnFutureInterruptible(f, callTimeoutMillis, TimeUnit.MILLISECONDS);
                } catch (HazelcastInstanceNotActiveException e) {
                    EmptyStatement.ignore(e);
                } catch (Exception e) {
                    logger.severe("While merging...", e);
                }
            }
        }

        private <V> V waitOnFutureInterruptible(Future<V> future, long timeout, TimeUnit timeUnit)
                throws ExecutionException, InterruptedException, TimeoutException {

            isNotNull(timeUnit, "timeUnit");
            long deadline = Clock.currentTimeMillis() + timeUnit.toMillis(timeout);
            while (true) {
                long localTimeoutMs = Math.min(MIN_WAIT_ON_FUTURE_TIMEOUT_MILLIS, deadline);
                try {
                    return future.get(localTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (TimeoutException t) {
                    deadline -= localTimeoutMs;
                    if (deadline <= 0) {
                        throw t;
                    }
                    if (!node.isActive()) {
                        future.cancel(true);
                        throw new HazelcastInstanceNotActiveException();
                    }
                }
            }
        }
    }
}
