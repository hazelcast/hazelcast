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
import com.hazelcast.cluster.impl.operations.JoinCheckOperation;
import com.hazelcast.cluster.impl.operations.JoinRequestOperation;
import com.hazelcast.cluster.impl.operations.MasterConfirmationOperation;
import com.hazelcast.cluster.impl.operations.MasterDiscoveryOperation;
import com.hazelcast.cluster.impl.operations.MemberCapabilityChangedOperation;
import com.hazelcast.cluster.impl.operations.MemberCapabilityUpdateException;
import com.hazelcast.cluster.impl.operations.MemberInfoUpdateOperation;
import com.hazelcast.cluster.impl.operations.MemberRemoveOperation;
import com.hazelcast.cluster.impl.operations.PostJoinOperation;
import com.hazelcast.cluster.impl.operations.SetMasterOperation;
import com.hazelcast.cluster.impl.operations.TriggerMemberListPublishOperation;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.Capability;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
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
import static com.hazelcast.instance.Capability.PARTITION_HOST;
import static com.hazelcast.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public final class ClusterServiceImpl implements ClusterService, ConnectionListener, ManagedService,
        EventPublishingService<MembershipEvent, MembershipListener> {

    public static final String SERVICE_NAME = "hz:core:clusterService";

    private static final String EXECUTOR_NAME = "hz:cluster";
    private static final int HEARTBEAT_INTERVAL = 500;
    private static final long HEARTBEAT_LOG_THRESHOLD = 10000L;
    private static final int PING_INTERVAL = 5000;

    private final Node node;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    protected final Address thisAddress;

    protected final MemberImpl thisMember;

    private final long waitMillisBeforeJoin;

    private final long maxWaitMillisBeforeJoin;

    private final long heartbeatInterval;

    private final long maxNoHeartbeatMillis;

    private final long maxNoMasterConfirmationMillis;

    private final boolean icmpEnabled;

    private final int icmpTtl;

    private final int icmpTimeout;

    private final Lock lock = new ReentrantLock();

    private final Set<MemberInfo> setJoins = new LinkedHashSet<MemberInfo>(100);

    private final AtomicReference<Map<Address, MemberImpl>> membersMapRef
            = new AtomicReference<Map<Address, MemberImpl>>(Collections.<Address, MemberImpl>emptyMap());

    private final AtomicReference<Set<MemberImpl>> membersRef = new AtomicReference<Set<MemberImpl>>(Collections.<MemberImpl>emptySet());

    private final AtomicBoolean preparingToMerge = new AtomicBoolean(false);

    private volatile boolean joinInProgress = false;

    private volatile long lastHeartBeat = 0L;

    private long timeToStartJoin = 0;

    private long firstJoinRequest = 0;

    private final ConcurrentMap<MemberImpl, Long> masterConfirmationTimes = new ConcurrentHashMap<MemberImpl, Long>();

    private final ExceptionHandler whileFinalizeJoinsExceptionHandler;

    private final ClusterClockImpl clusterClock;

    private String clusterId = null;

    public ClusterServiceImpl(final Node node) {
        this.node = node;
        nodeEngine = node.nodeEngine;
        logger = node.getLogger(ClusterService.class.getName());
        clusterClock = new ClusterClockImpl(logger);
        whileFinalizeJoinsExceptionHandler =
                logAllExceptions(logger, "While waiting finalize join calls...", Level.WARNING);
        thisAddress = node.getThisAddress();
        thisMember = node.getLocalMember();
        setMembers(thisMember);
        waitMillisBeforeJoin = node.groupProperties.WAIT_SECONDS_BEFORE_JOIN.getInteger() * 1000L;
        maxWaitMillisBeforeJoin = node.groupProperties.MAX_WAIT_SECONDS_BEFORE_JOIN.getInteger() * 1000L;
        long heartbeatIntervalSeconds = node.groupProperties.HEARTBEAT_INTERVAL_SECONDS.getInteger();
        heartbeatIntervalSeconds = heartbeatIntervalSeconds <= 0 ? 1 : heartbeatIntervalSeconds;
        heartbeatInterval = heartbeatIntervalSeconds;
        maxNoHeartbeatMillis = node.groupProperties.MAX_NO_HEARTBEAT_SECONDS.getInteger() * 1000L;
        maxNoMasterConfirmationMillis = node.groupProperties.MAX_NO_MASTER_CONFIRMATION_SECONDS.getInteger() * 1000L;
        icmpEnabled = node.groupProperties.ICMP_ENABLED.getBoolean();
        icmpTtl = node.groupProperties.ICMP_TTL.getInteger();
        icmpTimeout = node.groupProperties.ICMP_TIMEOUT.getInteger();
        node.connectionManager.addConnectionListener(this);
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
    public void init(final NodeEngine nodeEngine, Properties properties) {
        long mergeFirstRunDelay = node.getGroupProperties().MERGE_FIRST_RUN_DELAY_SECONDS.getLong() * 1000;
        mergeFirstRunDelay = mergeFirstRunDelay <= 0 ? 100 : mergeFirstRunDelay; // milliseconds

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(EXECUTOR_NAME, 2, 1000, ExecutorType.CACHED);

        long mergeNextRunDelay = node.getGroupProperties().MERGE_NEXT_RUN_DELAY_SECONDS.getLong() * 1000;
        mergeNextRunDelay = mergeNextRunDelay <= 0 ? 100 : mergeNextRunDelay; // milliseconds
        executionService.scheduleWithFixedDelay(EXECUTOR_NAME, new SplitBrainHandler(node),
                mergeFirstRunDelay, mergeNextRunDelay, TimeUnit.MILLISECONDS);

        executionService.scheduleWithFixedDelay(EXECUTOR_NAME, new Runnable() {
            public void run() {
                heartBeater();
            }
        }, heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);

        long masterConfirmationInterval = node.groupProperties.MASTER_CONFIRMATION_INTERVAL_SECONDS.getInteger();
        masterConfirmationInterval = masterConfirmationInterval <= 0 ? 1 : masterConfirmationInterval;
        executionService.scheduleWithFixedDelay(EXECUTOR_NAME, new Runnable() {
            public void run() {
                sendMasterConfirmation();
            }
        }, masterConfirmationInterval, masterConfirmationInterval, TimeUnit.SECONDS);

        long memberListPublishInterval = node.groupProperties.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getInteger();
        memberListPublishInterval = memberListPublishInterval <= 0 ? 1 : memberListPublishInterval;
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

    public JoinRequest checkJoinInfo(Address target) {
        Future f = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                new JoinCheckOperation(node.createJoinRequest()), target)
                .setTryCount(1).invoke();
        try {
            return (JoinRequest) nodeEngine.toObject(f.get());
        } catch (Exception e) {
            logger.warning("Error during join check!", e);
        }
        return null;
    }

    public boolean validateJoinMessage(JoinMessage joinMessage) throws Exception {
        boolean valid = Packet.VERSION == joinMessage.getPacketVersion();
        if (valid) {
            try {
                ConfigCheck newMemberConfigCheck = joinMessage.getConfigCheck();
                ConfigCheck clusterConfigCheck = node.createConfigCheck();
                valid = clusterConfigCheck.isCompatible(newMemberConfigCheck);
            } catch (Exception e) {
                final String message = "Invalid join request from: " + joinMessage.getAddress() + ", reason:" + e.getMessage();
                logger.warning(message);
                throw e;
            }
        }
        return valid;
    }

    private boolean isValidJoinMessage(JoinMessage joinMessage) {
        boolean validJoinRequest;
        try {
            validJoinRequest = validateJoinMessage(joinMessage);
        } catch (ConfigMismatchException e) {
            throw e;
        } catch (Exception e) {
            validJoinRequest = false;
        }
        return validJoinRequest;
    }

    private void logIfConnectionToEndpointIsMissing(long now, MemberImpl member) {
        if ((now - member.getLastRead()) >= PING_INTERVAL && (now - member.getLastPing()) >= PING_INTERVAL) {
            Connection conn = node.connectionManager.getOrConnect(member.getAddress());
            if (conn == null || !conn.isAlive()) {
                logger.warning("This node does not have a connection to " + member);
            }
        }
    }

    private void heartBeater() {
        if (!node.joined() || !node.isActive()) {
            return;
        }

        long now = Clock.currentTimeMillis();

        /*
         * Compensate for any abrupt jumps forward in the system clock.
         */
        long clockJump = 0L;
        if (lastHeartBeat != 0L) {
            clockJump = now - lastHeartBeat - TimeUnit.SECONDS.toMillis(heartbeatInterval);
            if (Math.abs(clockJump) > HEARTBEAT_LOG_THRESHOLD) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                logger.info("System clock apparently jumped from " + sdf.format(new Date(lastHeartBeat)) + " to " +
                        sdf.format(new Date(now)) + " since last heartbeat (" + String.format("%+d", clockJump) + "ms).");
            }
            clockJump = Math.max(0L, clockJump);

            if (clockJump >= maxNoMasterConfirmationMillis / 2) {
                logger.warning(
                        "Resetting master confirmation timestamps because of huge system clock jump! " + "Clock-Jump: "
                                + clockJump + "ms, Master-Confirmation-Timeout: " + maxNoMasterConfirmationMillis
                                + "ms.");
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
        Collection<MemberImpl> members = getMemberList();
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
                    sendHearBeatIfRequired(now, member);
                } catch (Throwable e) {
                    logger.severe(e);
                }
            }
        }
    }

    private boolean removeMemberIfNotHeartBeating(long now, MemberImpl member) {
        if ((now - member.getLastRead()) > maxNoHeartbeatMillis) {
            logger.warning("Removing " + member + " because it has not sent any heartbeats for " +
                    maxNoHeartbeatMillis + " ms.");
            removeAddress(member.getAddress());
            return true;
        }
        return false;
    }

    private boolean removeMemberIfMasterConfirmationExpired(long now, MemberImpl member) {
        Long lastConfirmation = masterConfirmationTimes.get(member);
        if (lastConfirmation == null ||
                (now - lastConfirmation > maxNoMasterConfirmationMillis)) {
            logger.warning("Removing " + member + " because it has not sent any master confirmation " +
                    " for " + maxNoMasterConfirmationMillis + " ms.");
            removeAddress(member.getAddress());
            return true;
        }
        return false;
    }

    private void heartBeaterSlave(long now, long clockJump) {
        Collection<MemberImpl> members = getMemberList();

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
                    sendHearBeatIfRequired(now, member);
                } catch (Throwable e) {
                    logger.severe(e);
                }
            }
        }
    }

    private boolean isMaster(MemberImpl member) {
        return member.getAddress().equals(getMasterAddress());
    }

    private void sendHearBeatIfRequired(long now, MemberImpl member) {
        if ((now - member.getLastWrite()) > HEARTBEAT_INTERVAL) {
            sendHeartbeat(member.getAddress());
        }
    }

    private void pingMemberIfRequired(long now, MemberImpl member) {
        if ((now - member.getLastRead()) >= PING_INTERVAL && (now - member.getLastPing()) >= PING_INTERVAL) {
            ping(member);
        }
    }

    private void ping(final MemberImpl memberImpl) {
        memberImpl.didPing();
        if (!icmpEnabled) {
            return;
        }
        nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
            public void run() {
                try {
                    final Address address = memberImpl.getAddress();
                    logger.warning(thisAddress + " will ping " + address);
                    for (int i = 0; i < 5; i++) {
                        try {
                            if (address.getInetAddress().isReachable(null, icmpTtl, icmpTimeout)) {
                                logger.info(thisAddress + " pings successfully. Target: " + address);
                                return;
                            }
                        } catch (ConnectException ignored) {
                            // no route to host
                            // means we cannot connect anymore
                        }
                    }
                    logger.warning(thisAddress + " couldn't ping " + address);
                    // not reachable.
                    removeAddress(address);
                } catch (Throwable ignored) {
                }
            }
        });
    }

    private void sendHeartbeat(Address target) {
        if (target == null) return;
        try {
            node.nodeEngine.getOperationService().send(new HeartbeatOperation(), target);
        } catch (Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest("Error while sending heartbeat -> "
                        + e.getClass().getName() + "[" + e.getMessage() + "]");
            }
        }
    }

    private void sendMasterConfirmation() {
        if (!node.joined() || !node.isActive() || isMaster()) {
            return;
        }
        final Address masterAddress = getMasterAddress();
        if (masterAddress == null) {
            logger.finest("Could not send MasterConfirmation, master is null!");
            return;
        }
        final MemberImpl masterMember = getMember(masterAddress);
        if (masterMember == null) {
            logger.finest("Could not send MasterConfirmation, master is null!");
            return;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Sending MasterConfirmation to " + masterMember);
        }
        nodeEngine.getOperationService().send(new MasterConfirmationOperation(), masterAddress);
    }

    // Will be called just before this node becomes the master
    private void resetMemberMasterConfirmations() {
        final Collection<MemberImpl> memberList = getMemberList();
        for (MemberImpl member : memberList) {
            masterConfirmationTimes.put(member, Clock.currentTimeMillis());
        }
    }


    public void sendMemberListToMember(Address target) {
        if (!isMaster()) {
            return;
        }
        if (thisAddress.equals(target)) {
            return;
        }
        final Collection<MemberImpl> members = getMemberList();
        MemberInfoUpdateOperation op = new MemberInfoUpdateOperation(
                createMemberInfoList(members), clusterClock.getClusterTime(), false);
        nodeEngine.getOperationService().send(op, target);
    }

    private void sendMemberListToOthers() {
        if (!isMaster()) {
            return;
        }
        final Collection<MemberImpl> members = getMemberList();
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
        if (preparingToMerge.get()) {
            logger.warning("Cluster-merge process is ongoing, won't process member removal: " + deadAddress);
            return;
        }
        if (!node.joined()) {
            return;
        }
        if (deadAddress.equals(thisAddress)) {
            return;
        }
        lock.lock();
        try {
            if (deadAddress.equals(node.getMasterAddress())) {
                assignNewMaster();
            }
            if (node.isMaster()) {
                setJoins.remove(new MemberInfo(deadAddress));
                resetMemberMasterConfirmations();
            }
            final Connection conn = node.connectionManager.getConnection(deadAddress);
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

    private void assignNewMaster() {
        final Address oldMasterAddress = node.getMasterAddress();
        if (node.joined()) {
            final Collection<MemberImpl> members = getMemberList();
            MemberImpl newMaster = null;
            final int size = members.size();
            if (size > 1) {
                final Iterator<MemberImpl> iter = members.iterator();
                final MemberImpl member = iter.next();
                if (member.getAddress().equals(oldMasterAddress)) {
                    newMaster = iter.next();
                } else {
                    logger.severe("Old master " + oldMasterAddress
                            + " is dead but the first of member list is a different member " +
                            member + "!");
                    newMaster = member;
                }
            } else {
                logger.warning("Old master is dead and this node is not master " +
                        "but member list contains only " + size + " members! -> " + members);
            }
            logger.info("Master " + oldMasterAddress + " left the cluster. Assigning new master " + newMaster);
            if (newMaster != null) {
                node.setMasterAddress(newMaster.getAddress());
            } else {
                node.setMasterAddress(null);
            }
        } else {
            node.setMasterAddress(null);
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Now Master " + node.getMasterAddress());
        }
    }

    public void answerMasterQuestion(JoinMessage joinMessage) {
        if (!ensureValidConfiguration(joinMessage)) {
            return;
        }

        if (node.getMasterAddress() != null) {
            sendMasterAnswer(joinMessage.getAddress());
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Received a master question from " + joinMessage.getAddress()
                        + ", but this node is not master itself or doesn't have a master yet!");
            }
        }
    }

    public void handleJoinRequest(JoinRequest joinRequest, Connection connection) {
        if (!node.joined() || !node.isActive()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Node is not ready to process join request...");
            }
            return;
        }

        if (!ensureValidConfiguration(joinRequest)) {
            return;
        }

        Address target = joinRequest.getAddress();
        boolean isRequestFromCurrentMaster = target.equals(node.getMasterAddress());
        // If the join request from current master, don't send a master answer
        // because master can somehow dropped its connection and wants to join back.
        if (!node.isMaster() && !isRequestFromCurrentMaster) {
            sendMasterAnswer(target);
            return;
        }

        if (joinInProgress) {
            if (logger.isFinestEnabled()) {
                logger.finest("Join is in-progress. Cannot handle join request from "
                        + target + " at the moment.");
            }
            return;
        }

        lock.lock();
        try {
            if (isJoinRequestFromAnExistingMember(joinRequest, connection)) {
                return;
            }

            long now = Clock.currentTimeMillis();
            if (logger.isFinestEnabled()) {
                String msg = "Handling join from " + target + ", inProgress: " + joinInProgress
                        + (timeToStartJoin > 0 ? ", timeToStart: " + (timeToStartJoin - now) : "");
                logger.finest(msg);
            }

            MemberInfo memberInfo = new MemberInfo(target, joinRequest.getUuid(), joinRequest.getCapabilities(),
                    joinRequest.getAttributes());

            if (!setJoins.contains(memberInfo)) {
                try {
                    checkSecureLogin(joinRequest, memberInfo);
                } catch (Exception e) {
                    ILogger securityLogger = node.loggingService.getLogger("com.hazelcast.security");
                    sendAuthenticationFailure(target);
                    securityLogger.severe(e);
                    return;
                }
            }

            if (node.isMaster()) {
                try {
                    node.getNodeExtension().beforeJoin();
                } catch (Exception e) {
                    logger.warning(e.getMessage());
                    sendBeforeJoinCheckFailure(target, e.getMessage());
                    return;
                }
            }

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
        } finally {
            lock.unlock();
        }
    }

    private boolean ensureValidConfiguration(JoinMessage joinMessage) {
        try {
            if (isValidJoinMessage(joinMessage)) {
                return true;
            }

            logger.warning("Received an invalid join request from " + joinMessage.getAddress()
                    + ", cause: clusters part of different cluster-groups");

            nodeEngine.getOperationService().send(new GroupMismatchOperation(), joinMessage.getAddress());
        } catch (ConfigMismatchException e) {
            logger.warning("Received an invalid join request from " + joinMessage.getAddress()
                    + ", cause:" + e.getMessage());
            sendConfigurationMismatchFailure(joinMessage.getAddress(), e.getMessage());
        }
        return false;
    }

    private void sendAuthenticationFailure(Address target) {
        nodeEngine.getOperationService().send(new AuthenticationFailureOperation(), target);
    }

    private void sendBeforeJoinCheckFailure(Address target, String message) {
        nodeEngine.getOperationService().send(new BeforeJoinCheckFailureOperation(message), target);
    }

    private void sendConfigurationMismatchFailure(Address target, String msg) {
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(new ConfigMismatchOperation(msg), target);
    }

    private void checkSecureLogin(JoinRequest joinRequest, MemberInfo newMemberInfo) {
        if (node.securityContext != null && !setJoins.contains(newMemberInfo)) {
            Credentials cr = joinRequest.getCredentials();
            if (cr == null) {
                throw new SecurityException("Expecting security credentials " +
                        "but credentials could not be found in JoinRequest!");
            }

            try {
                LoginContext lc = node.securityContext.createMemberLoginContext(cr);
                lc.login();
            } catch (LoginException e) {
                throw new SecurityException(
                        "Authentication has failed for " + cr.getPrincipal() + '@' + cr.getEndpoint()
                                + " => (" + e.getMessage() + ")");
            }
        }
    }

    private boolean isJoinRequestFromAnExistingMember(JoinRequest joinRequest, Connection connection) {
        MemberImpl member = getMember(joinRequest.getAddress());
        if (member == null) {
            return false;
        }

        Address target = member.getAddress();
        if (joinRequest.getUuid().equals(member.getUuid())) {
            if (node.isMaster()) {
                if (logger.isFinestEnabled()) {
                    String message = "Ignoring join request, member already exists.. => " + joinRequest;
                    logger.finest(message);
                }
                // send members update back to node trying to join again...
                final Operation[] postJoinOps = nodeEngine.getPostJoinOperations();
                final PostJoinOperation postJoinOp = postJoinOps != null && postJoinOps.length > 0
                        ? new PostJoinOperation(postJoinOps) : null;

                Operation op = new FinalizeJoinOperation(createMemberInfoList(getMemberList()), postJoinOp,
                        clusterClock.getClusterTime(), false);
                nodeEngine.getOperationService().send(op, target);
            } else {
                sendMasterAnswer(target);
            }
            return true;
        }

        // If this node is master then remove old member and process join request.
        // If requesting address is equal to master node's address, that means master node
        // somehow disconnected and wants to join back.
        // So drop old member and process join request if this node becomes master.
        if (node.isMaster() || target.equals(node.getMasterAddress())) {
            logger.warning("New join request has been received from an existing endpoint! => " + member
                    + " Removing old member and processing join request...");

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
            logger.info("Cannot send master answer to " + target + " since master node is not known yet");
            return;
        }
        SetMasterOperation op = new SetMasterOperation(masterAddress);
        nodeEngine.getOperationService().send(op, target);
    }

    public void handleMaster(Address masterAddress, Address callerAddress) {
        if (node.joined()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Ignoring master response: " + masterAddress + " from: " + callerAddress
                        + ". This node is already joined...");
            }
            return;
        }

        if (node.getThisAddress().equals(masterAddress)) {
            if (node.isMaster()) {
                logger.finest("Ignoring master response: " + masterAddress + " from: " + callerAddress
                        + ". This node is already master...");
            } else {
                node.setAsMaster();
            }
            return;
        }

        lock.lock();
        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Handling master response: " + masterAddress + " from: " + callerAddress);
            }
            final Address currentMaster = node.getMasterAddress();
            if (currentMaster == null || currentMaster.equals(masterAddress)) {
                setMasterAndJoin(masterAddress);
            } else if (currentMaster.equals(callerAddress)) {
                logger.info("Setting master to: " + masterAddress + ", since "
                        + currentMaster + " says it's not master anymore.");
                setMasterAndJoin(masterAddress);
            } else {
                final Connection conn = node.connectionManager.getConnection(currentMaster);
                if (conn != null && conn.isAlive()) {
                    logger.info("Ignoring master response: " + masterAddress + " from: " + callerAddress
                            + ", since this node has an active master: " + currentMaster);
                    sendJoinRequest(currentMaster, true);
                } else {
                    logger.warning("Ambiguous master response, this node has a master: " + currentMaster
                            + ", but doesn't have a connection to. " + callerAddress + " sent master response as: "
                            + masterAddress + ". Master field will be unset now...");
                    node.setMasterAddress(null);
                }
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

    public void acceptMasterConfirmation(MemberImpl member) {
        if (member != null) {
            if (logger.isFinestEnabled()) {
                logger.finest("MasterConfirmation has been received from " + member);
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
        }, 10, TimeUnit.SECONDS);
    }

    public void merge(Address newTargetAddress) {
        if (preparingToMerge.compareAndSet(true, false)) {
            node.getJoiner().setTargetAddress(newTargetAddress);
            final LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
            lifecycleService.runUnderLifecycleLock(new Runnable() {
                public void run() {
                    lifecycleService.fireLifecycleEvent(MERGING);
                    final NodeEngineImpl nodeEngine = node.nodeEngine;
                    final Collection<SplitBrainHandlerService> services = nodeEngine
                            .getServices(SplitBrainHandlerService.class);
                    final Collection<Runnable> tasks = new LinkedList<Runnable>();
                    for (SplitBrainHandlerService service : services) {
                        final Runnable runnable = service.prepareMergeRunnable();
                        if (runnable != null) {
                            tasks.add(runnable);
                        }
                    }
                    final Collection<ManagedService> managedServices = nodeEngine.getServices(ManagedService.class);
                    for (ManagedService service : managedServices) {
                        service.reset();
                    }
                    node.onRestart();
                    node.nodeEngine.reset();
                    node.connectionManager.restart();
                    node.rejoin();
                    final Collection<Future> futures = new LinkedList<Future>();
                    for (Runnable task : tasks) {
                        Future f = nodeEngine.getExecutionService().submit("hz:system", task);
                        futures.add(f);
                    }
                    long callTimeout = node.groupProperties.OPERATION_CALL_TIMEOUT_MILLIS.getLong();
                    for (Future f : futures) {
                        try {
                            waitOnFutureInterruptible(f, callTimeout, TimeUnit.MILLISECONDS);
                        } catch (HazelcastInstanceNotActiveException e) {
                            EmptyStatement.ignore(e);
                        } catch (Exception e) {
                            logger.severe("While merging...", e);
                        }
                    }
                    if (node.isActive() && node.joined()) {
                        lifecycleService.fireLifecycleEvent(MERGED);
                    }
                }
            });
        }
    }

    private <V> V waitOnFutureInterruptible(Future<V> future, long timeout, TimeUnit timeUnit)
            throws ExecutionException, InterruptedException, TimeoutException {

        isNotNull(timeUnit, "timeUnit");
        long deadline = Clock.currentTimeMillis() + timeUnit.toMillis(timeout);
        while (true) {
            long localTimeout = Math.min(1000 * 10, deadline);
            try {
                return future.get(localTimeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                deadline -= localTimeout;
                if (deadline <= 0) {
                    throw te;
                }
                if (!node.isActive()) {
                    future.cancel(true);
                    throw new HazelcastInstanceNotActiveException();
                }
            }
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
        logger.finest("Starting Join.");
        lock.lock();
        try {
            try {
                joinInProgress = true;
                // pause migrations until join, member-update and post-join operations are completed.
                node.getPartitionService().pauseMigration();
                final Collection<MemberImpl> members = getMemberList();
                final Collection<MemberInfo> memberInfos = createMemberInfoList(members);
                for (MemberInfo memberJoining : setJoins) {
                    memberInfos.add(memberJoining);
                }
                final long time = clusterClock.getClusterTime();
                // Post join operations must be lock free; means no locks at all;
                // no partition locks, no key-based locks, no service level locks!
                final Operation[] postJoinOps = nodeEngine.getPostJoinOperations();
                final PostJoinOperation postJoinOp = postJoinOps != null && postJoinOps.length > 0
                        ? new PostJoinOperation(postJoinOps) : null;
                final int count = members.size() - 1 + setJoins.size();
                final List<Future> calls = new ArrayList<Future>(count);
                for (MemberInfo member : setJoins) {
                    final long startTime = clusterClock.getClusterStartTime();
                    calls.add(invokeClusterOperation(new FinalizeJoinOperation(memberInfos, postJoinOp, time,
                            clusterId, startTime), member.getAddress()));
                }
                for (MemberImpl member : members) {
                    if (!member.getAddress().equals(thisAddress)) {
                        calls.add(invokeClusterOperation(new MemberInfoUpdateOperation(memberInfos, time, true), member.getAddress()));
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
        final List<MemberInfo> memberInfos = new LinkedList<MemberInfo>();
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member));
        }
        return memberInfos;
    }

    private static Set<MemberInfo> createMemberInfoSet(Collection<MemberImpl> members) {
        final Set<MemberInfo> memberInfos = new HashSet<MemberInfo>();
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

            MemberImpl[] newMembers = new MemberImpl[members.size()];
            int k = 0;
            for (MemberInfo memberInfo : members) {
                MemberImpl member = currentMemberMap.get(memberInfo.getAddress());
                if (member == null) {
                    member = createMember(memberInfo.getAddress(), memberInfo.getUuid(),
                            thisAddress.getScopeId(), memberInfo.getCapabilities(), memberInfo.getAttributes());
                }
                newMembers[k++] = member;
                member.didRead();
            }
            setMembers(newMembers);
            if (!getMemberList().contains(thisMember)) {
                throw new HazelcastException("Member list doesn't contain local member!");
            }
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
            logger.warning("Received an inconsistent member update, it has more members but "
                    + "but also removes some of the current members! "
                    + "Ignoring and requesting a new member update...");
            nodeEngine.getOperationService().send(new TriggerMemberListPublishOperation(), getMasterAddress());
            return false;
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

    public void updateMemberCapabilities(String uuid, Set<Capability> newCapabilities) {
        lock.lock();
        try {
            MemberImpl member = getMember(uuid);
            if (member == null) {
                throw new MemberCapabilityUpdateException(String.format("Could not find member [%s] to update capabilities", uuid));
            }

            if (node.isMaster()) {
                doMasterUpdate(member, newCapabilities);
            } else {
                member.setCapabilities(newCapabilities);
            }
        } finally {
            lock.unlock();
        }
    }

    private void doMasterUpdate(MemberImpl member, Set<Capability> newCapabilities) {
        if (canUpdate(newCapabilities)) {
            boolean wasPartitionHost = member.hasCapability(PARTITION_HOST);

            if (member.setCapabilities(newCapabilities)) {
                // If node has been added or removed as a partition host then trigger re-partitioning
                if (wasPartitionHost != member.hasCapability(PARTITION_HOST)) {
                    node.getPartitionService().memberCapabilityUpdate(member);
                }

                logger.info(String.format("Updated member [%s] capabilities to [%s]", member, newCapabilities));

                // Let other members know they should update their members
                notifyCapabilityUpdate(member.getUuid(), newCapabilities);
            }
        } else {
            logger.info(String.format("Cannot update member [%s] capabilities to [%s]", member, newCapabilities));
        }
    }

    private boolean canUpdate(Set<Capability> newCapabilities) {
        return newCapabilities.contains(PARTITION_HOST) || getMemberList(PARTITION_HOST).size() > 1;
    }

    private void notifyCapabilityUpdate(String uuid, Set<Capability> capabilities) {
        for (MemberImpl member : getMemberList()) {
            if (!member.localMember()) {
                invokeClusterOperation(new MemberCapabilityChangedOperation(uuid, capabilities), member.getAddress());
            }
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
                thisMember.getUuid(), node.createConfigCheck(), getSize());
        return nodeEngine.getOperationService().send(new MasterDiscoveryOperation(joinMessage), toAddress);
    }

    @Override
    public void connectionAdded(final Connection connection) {
        MemberImpl member = getMember(connection.getEndPoint());
        if (member != null) {
            member.didRead();
        }
    }

    @Override
    public void connectionRemoved(Connection connection) {
        if (logger.isFinestEnabled()) {
            logger.finest("Connection is removed " + connection.getEndPoint());
        }
        if (!node.joined()) {
            final Address masterAddress = node.getMasterAddress();
            if (masterAddress != null && masterAddress.equals(connection.getEndPoint())) {
                node.setMasterAddress(null);
            }
        }
    }

    private Future invokeClusterOperation(Operation op, Address target) {
        return nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, op, target)
                .setTryCount(100).invoke();
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    private void setMembers(MemberImpl... members) {
        if (members == null || members.length == 0) return;
        if (logger.isFinestEnabled()) {
            logger.finest("Updating members -> " + Arrays.toString(members));
        }
        lock.lock();
        try {
            Map<Address, MemberImpl> oldMemberMap = membersMapRef.get();
            final Map<Address, MemberImpl> memberMap = new LinkedHashMap<Address, MemberImpl>();  // ! ORDERED !
            final Collection<MemberImpl> newMembers = new LinkedList<MemberImpl>();
            for (MemberImpl member : members) {
                MemberImpl currentMember = oldMemberMap.get(member.getAddress());
                if (currentMember == null) {
                    newMembers.add(member);
                    masterConfirmationTimes.put(member, Clock.currentTimeMillis());
                }
                memberMap.put(member.getAddress(), member);
            }
            setMembersRef(memberMap);

            if (!newMembers.isEmpty()) {
                Set<Member> eventMembers = new LinkedHashSet<Member>(oldMemberMap.values());
                if (newMembers.size() == 1) {
                    MemberImpl newMember = newMembers.iterator().next();
                    node.getPartitionService().memberAdded(newMember); // sync call
                    eventMembers.add(newMember);
                    sendMembershipEventNotifications(newMember, unmodifiableSet(eventMembers), true); // async events
                } else {
                    for (MemberImpl newMember : newMembers) {
                        node.getPartitionService().memberAdded(newMember); // sync call
                        eventMembers.add(newMember);
                        sendMembershipEventNotifications(newMember, unmodifiableSet(new LinkedHashSet<Member>(eventMembers)), true); // async events
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void removeMember(MemberImpl deadMember) {
        logger.info("Removing " + deadMember);
        lock.lock();
        try {
            final Map<Address, MemberImpl> members = membersMapRef.get();
            if (members.containsKey(deadMember.getAddress())) {
                Map<Address, MemberImpl> newMembers = new LinkedHashMap<Address, MemberImpl>(members);  // ! ORDERED !
                newMembers.remove(deadMember.getAddress());
                masterConfirmationTimes.remove(deadMember);
                setMembersRef(newMembers);
                node.getPartitionService().memberRemoved(deadMember); // sync call
                nodeEngine.onMemberLeft(deadMember);                  // sync call
                if (node.isMaster()) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(deadMember + " is dead. Sending remove to all other members.");
                    }
                    invokeMemberRemoveOperation(deadMember.getAddress());
                }
                // async events
                sendMembershipEventNotifications(deadMember, unmodifiableSet(new LinkedHashSet<Member>(newMembers.values())), false);
            }
        } finally {
            lock.unlock();
        }
    }

    private void invokeMemberRemoveOperation(final Address deadAddress) {
        for (MemberImpl member : getMemberList()) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address) && !address.equals(deadAddress)) {
                nodeEngine.getOperationService().send(new MemberRemoveOperation(deadAddress), address);
            }
        }
    }

    public void sendShutdownMessage() {
        invokeMemberRemoveOperation(thisAddress);
    }

    private void sendMembershipEventNotifications(final MemberImpl member, Set<Member> members, final boolean added) {
        final int eventType = added ? MembershipEvent.MEMBER_ADDED : MembershipEvent.MEMBER_REMOVED;
        final MembershipEvent membershipEvent = new MembershipEvent(getClusterProxy(), member, eventType, members);
        final Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        if (membershipAwareServices != null && !membershipAwareServices.isEmpty()) {
            final MembershipServiceEvent event = new MembershipServiceEvent(membershipEvent);
            for (final MembershipAwareService service : membershipAwareServices) {
                // service events should not block each other
                nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
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
        final EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        for (EventRegistration reg : registrations) {
            eventService.publishEvent(SERVICE_NAME, reg, membershipEvent, reg.getId().hashCode());
        }
    }

    private void sendMemberAttributeEvent(MemberImpl member, MemberAttributeOperationType operationType, String key, Object value) {
        final MemberAttributeEvent memberAttributeEvent = new MemberAttributeEvent(getClusterProxy(), member, operationType, key, value);
        final Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        final MemberAttributeServiceEvent event = new MemberAttributeServiceEvent(getClusterProxy(), member, operationType, key, value);
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
        final EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        for (EventRegistration reg : registrations) {
            eventService.publishEvent(SERVICE_NAME, reg, memberAttributeEvent, reg.getId().hashCode());
        }
    }

    protected MemberImpl createMember(Address address, String nodeUuid, String ipV6ScopeId, Set<Capability> capabilities,
                                      Map<String, Object> attributes) {

        address.setScopeId(ipV6ScopeId);
        return new MemberImpl(address, thisAddress.equals(address), nodeUuid,
                (HazelcastInstanceImpl) nodeEngine.getHazelcastInstance(), capabilities, attributes);
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
    public Collection<MemberImpl> getMemberList() {
        return membersRef.get();
    }

    public Collection<MemberImpl> getMemberList(Capability filter) {
        Collection<MemberImpl> memberList = getMemberList();
        Collection<MemberImpl> filtered = new ArrayList<MemberImpl>(memberList.size());
        for (MemberImpl member : memberList) {
            if (member.hasCapability(filter)) {
                filtered.add(member);
            }
        }

        return filtered;
    }

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

    @Override
    public int getSize() {
        final Collection<MemberImpl> members = getMemberList();
        return members != null ? members.size() : 0;
    }

    public String addMembershipListener(MembershipListener listener) {
        checkNotNull(listener, "listener can't be null");

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
        checkNotNull(registrationId, "registrationId can't be null");

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
                throw new IllegalArgumentException("Unhandled event:" + event);
        }
    }

    public Cluster getClusterProxy() {
        return new ClusterProxy(this);
    }

    public String membersString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        final Collection<MemberImpl> members = getMemberList();
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
        final StringBuilder sb = new StringBuilder();
        sb.append("ClusterService");
        sb.append("{address=").append(thisAddress);
        sb.append('}');
        return sb.toString();
    }
}
