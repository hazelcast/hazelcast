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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.AuthenticationFailureOp;
import com.hazelcast.internal.cluster.impl.operations.BeforeJoinCheckFailureOp;
import com.hazelcast.internal.cluster.impl.operations.ClusterMismatchOp;
import com.hazelcast.internal.cluster.impl.operations.ConfigMismatchOp;
import com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOp;
import com.hazelcast.internal.cluster.impl.operations.JoinRequestOp;
import com.hazelcast.internal.cluster.impl.operations.MasterResponseOp;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.cluster.impl.operations.WhoisMasterOp;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.cluster.impl.MemberMap.SINGLETON_MEMBER_LIST_VERSION;
import static com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage.SplitBrainMergeCheckResult.CANNOT_MERGE;
import static com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage.SplitBrainMergeCheckResult.LOCAL_NODE_SHOULD_MERGE;
import static com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage.SplitBrainMergeCheckResult.REMOTE_NODE_SHOULD_MERGE;
import static com.hazelcast.internal.hotrestart.InternalHotRestartService.PERSISTENCE_ENABLED_ATTRIBUTE;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * ClusterJoinManager manages member join process.
 * <p>
 * If this node is not master,
 * then it will answer with sending master node's address to a join request.
 * <p>
 * If this is master node, it will handle join request and notify all other members
 * about newly joined member.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:npathcomplexity"})
public class ClusterJoinManager {

    public static final String STALE_JOIN_PREVENTION_DURATION_PROP = "hazelcast.stale.join.prevention.duration.seconds";
    private static final int DEFAULT_STALE_JOIN_PREVENTION_DURATION_IN_SECS = 30;
    private static final int CLUSTER_OPERATION_RETRY_COUNT = 100;

    private final ILogger logger;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final Lock clusterServiceLock;
    private final ClusterClockImpl clusterClock;
    private final ClusterStateManager clusterStateManager;

    private final Map<Address, MemberInfo> joiningMembers = new LinkedHashMap<>();
    private final Map<UUID, Long> recentlyJoinedMemberUuids = new HashMap<>();

    /**
     * Recently left member UUIDs: when a recently crashed member is joining
     * with same UUID, typically it will have Persistence feature enabled
     * (otherwise it will restart probably on the same address but definitely
     * with a new random UUID). In order to support crashed members recovery
     * with Persistence, partition table validation does not expect an
     * identical partition table.
     *
     * Accessed by operation & cluster heartbeat threads
     */
    private final ConcurrentMap<UUID, Long> leftMembersUuids = new ConcurrentHashMap<>();
    private final long maxWaitMillisBeforeJoin;
    private final long waitMillisBeforeJoin;
    private final long staleJoinPreventionDurationInMillis;

    private long firstJoinRequest;
    private long timeToStartJoin;
    private volatile boolean joinInProgress;

    ClusterJoinManager(Node node, ClusterServiceImpl clusterService, Lock clusterServiceLock) {
        this.node = node;
        this.clusterService = clusterService;
        this.clusterServiceLock = clusterServiceLock;
        this.nodeEngine = clusterService.getNodeEngine();

        logger = node.getLogger(getClass());
        clusterStateManager = clusterService.getClusterStateManager();
        clusterClock = clusterService.getClusterClock();

        maxWaitMillisBeforeJoin = node.getProperties().getMillis(ClusterProperty.MAX_WAIT_SECONDS_BEFORE_JOIN);
        waitMillisBeforeJoin = node.getProperties().getMillis(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN);
        staleJoinPreventionDurationInMillis = TimeUnit.SECONDS.toMillis(
            Integer.getInteger(STALE_JOIN_PREVENTION_DURATION_PROP, DEFAULT_STALE_JOIN_PREVENTION_DURATION_IN_SECS));
    }

    boolean isJoinInProgress() {
        if (joinInProgress) {
            return true;
        }
        clusterServiceLock.lock();
        try {
            return joinInProgress || !joiningMembers.isEmpty();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    boolean isMastershipClaimInProgress() {
        clusterServiceLock.lock();
        try {
            return joinInProgress && joiningMembers.isEmpty();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    /**
     * Handle a {@link JoinRequestOp}. If this node is not master, reply with a {@link MasterResponseOp} to let the
     * joining node know the current master. Otherwise, if no other join is in progress, execute the {@link JoinRequest}
     *
     * @param joinRequest the join request
     * @param connection the connection to the joining node
     * @see JoinRequestOp
     */
    public void handleJoinRequest(JoinRequest joinRequest, ServerConnection connection) {
        if (!ensureNodeIsReady()) {
            return;
        }
        if (!ensureValidConfiguration(joinRequest)) {
            return;
        }

        Address target = joinRequest.getAddress();
        boolean isRequestFromCurrentMaster = target.equals(clusterService.getMasterAddress());
        // if the join request from current master, do not send a master answer,
        // because master can somehow dropped its connection and wants to join back
        if (!clusterService.isMaster() && !isRequestFromCurrentMaster) {
            sendMasterAnswer(target);
            return;
        }

        if (joinInProgress) {
            if (logger.isFineEnabled()) {
                logger.fine(format("Join or membership claim is in progress, cannot handle join request from %s at the moment",
                        target));
            }
            return;
        }

        executeJoinRequest(joinRequest, connection);
    }

    private boolean ensureNodeIsReady() {
        if (clusterService.isJoined() && node.isRunning()) {
            return true;
        }
        if (logger.isFineEnabled()) {
            logger.fine("Node is not ready to process join request...");
        }
        return false;
    }

    private boolean ensureValidConfiguration(JoinMessage joinMessage) {
        Address address = joinMessage.getAddress();
        try {
            if (isValidJoinMessage(joinMessage)) {
                return true;
            }

            logger.warning(format("Received an invalid join request from %s, cause: members part of different cluster",
                    address));
            nodeEngine.getOperationService().send(new ClusterMismatchOp(), address);
        } catch (ConfigMismatchException e) {
            logger.warning(format("Received an invalid join request from %s, cause: %s", address, e.getMessage()));
            OperationService operationService = nodeEngine.getOperationService();
            operationService.send(new ConfigMismatchOp(e.getMessage()), address);
        }
        return false;
    }

    // wraps validateJoinMessage to check configuration of this vs joining node,
    // rethrows only on ConfigMismatchException; in case of other exception, returns false.
    private boolean isValidJoinMessage(JoinMessage joinMessage) {
        try {
            return validateJoinMessage(joinMessage);
        } catch (ConfigMismatchException e) {
            throw e;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Validate that the configuration received from the remote node in {@code joinMessage} is compatible with the
     * configuration of this node.
     *
     * @param joinMessage the {@link JoinMessage} received from another node.
     * @return {@code true} if packet version of join message matches this node's packet version and configurations
     * are found to be compatible, otherwise {@code false}.
     * @throws Exception in case any exception occurred while checking compatibility
     * @see ConfigCheck
     */
    public boolean validateJoinMessage(JoinMessage joinMessage) {
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

    /**
     * Executed by a master node to process the {@link JoinRequest} sent by a node attempting to join the cluster.
     *
     * @param joinRequest the join request from a node attempting to join
     * @param connection  the connection of this node to the joining node
     */
    private void executeJoinRequest(JoinRequest joinRequest, ServerConnection connection) {
        clusterServiceLock.lock();
        try {
            if (checkJoinRequest(joinRequest, connection)) {
                return;
            }

            if (!authenticate(joinRequest, connection)) {
                return;
            }

            if (!validateJoinRequest(joinRequest, joinRequest.getAddress())) {
                return;
            }

            startJoinRequest(joinRequest.toMemberInfo());
        } finally {
            clusterServiceLock.unlock();
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private boolean checkJoinRequest(JoinRequest joinRequest, ServerConnection connection) {
        if (checkIfJoinRequestFromAnExistingMember(joinRequest, connection)) {
            return true;
        }

        final InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
        Address target = joinRequest.getAddress();
        UUID targetUuid = joinRequest.getUuid();

        if (hotRestartService.isMemberExcluded(target, targetUuid)) {
            logger.fine("cannot join " + target + " because it is excluded in cluster start.");
            hotRestartService.notifyExcludedMember(target);
            return true;
        }

        if (joinRequest.getExcludedMemberUuids().contains(clusterService.getThisUuid())) {
            logger.warning("cannot join " + target + " since this node is excluded in its list...");
            hotRestartService.handleExcludedMemberUuids(target, joinRequest.getExcludedMemberUuids());
            return true;
        }

        return checkClusterStateBeforeJoin(target, targetUuid);
    }

    private boolean checkClusterStateBeforeJoin(Address target, UUID uuid) {
        ClusterState state = clusterStateManager.getState();
        if (state == ClusterState.IN_TRANSITION) {
            logger.warning("Cluster state is in transition process. Join is not allowed until "
                    + "transaction is completed -> "
                    + clusterStateManager.stateToString());
            return true;
        }

        if (state.isJoinAllowed()) {
            return checkRecentlyJoinedMemberUuidBeforeJoin(target, uuid);
        }

        if (clusterService.isMissingMember(target, uuid)) {
            return false;
        }

        if (node.getNodeExtension().isStartCompleted()) {
            String message = "Cluster state either is locked or doesn't allow new members to join -> "
                    + clusterStateManager.stateToString();
            logger.warning(message);

            OperationService operationService = nodeEngine.getOperationService();
            BeforeJoinCheckFailureOp op = new BeforeJoinCheckFailureOp(message);
            operationService.send(op, target);
        } else {
            String message = "Cluster state either is locked or doesn't allow new members to join -> "
                    + clusterStateManager.stateToString() + ". Silently ignored join request of " + target
                    + " because start not completed.";
            logger.warning(message);
        }
        return true;
    }

    void insertIntoRecentlyJoinedMemberSet(Collection<? extends Member> members) {
        cleanupRecentlyJoinedMemberUuids();
        if (clusterService.getClusterState().isJoinAllowed()) {
            long localTime = Clock.currentTimeMillis();
            for (Member member : members) {
                recentlyJoinedMemberUuids.put(member.getUuid(), localTime);
            }
        }
    }

    private boolean checkRecentlyJoinedMemberUuidBeforeJoin(Address target, UUID uuid) {
        cleanupRecentlyJoinedMemberUuids();
        boolean recentlyJoined = recentlyJoinedMemberUuids.containsKey(uuid);
        if (recentlyJoined) {
            logger.warning("Cannot allow join request from " + target + ", since it has been already joined with " + uuid);
        }
        return recentlyJoined;
    }

    private void cleanupRecentlyJoinedMemberUuids() {
        long currentTime = Clock.currentTimeMillis();
        recentlyJoinedMemberUuids.values().removeIf(joinTime -> (currentTime - joinTime) >= staleJoinPreventionDurationInMillis);
    }

    private boolean authenticate(JoinRequest joinRequest, Connection connection) {
        if (!joiningMembers.containsKey(joinRequest.getAddress())) {
            try {
                secureLogin(joinRequest, connection);
            } catch (Exception e) {
                ILogger securityLogger = node.loggingService.getLogger("com.hazelcast.security");
                nodeEngine.getOperationService().send(new AuthenticationFailureOp(), joinRequest.getAddress());
                securityLogger.severe(e);
                return false;
            }
        }
        return true;
    }

    private void secureLogin(JoinRequest joinRequest, Connection connection) {
        if (node.securityContext != null) {
            Credentials credentials = joinRequest.getCredentials();
            if (credentials == null) {
                throw new SecurityException("Expecting security credentials, but credentials could not be found in join request");
            }
            String endpoint = joinRequest.getAddress().getHost();
            Boolean passed = Boolean.FALSE;
            try {
                String remoteClusterName = joinRequest.getConfigCheck().getClusterName();
                LoginContext loginContext = node.securityContext.createMemberLoginContext(remoteClusterName, credentials,
                        connection);
                loginContext.login();
                connection.attributeMap().put(LoginContext.class, loginContext);
                passed = Boolean.TRUE;
            } catch (LoginException e) {
                throw new SecurityException(format("Authentication has failed for %s @%s, cause: %s",
                        String.valueOf(credentials), endpoint, e.getMessage()));
            } finally {
                Address remoteAddr = connection == null ? null : connection.getRemoteAddress();
                nodeEngine.getNode().getNodeExtension().getAuditlogService()
                    .eventBuilder(AuditlogTypeIds.AUTHENTICATION_MEMBER)
                    .message("Member connection authentication.")
                    .addParameter("credentials", credentials)
                    .addParameter("remoteAddress", remoteAddr)
                    .addParameter("endpoint", endpoint)
                    .addParameter("passed", passed)
                    .log();
            }
        }
    }

    /**
     * Invoked from master node while executing a join request to validate it, delegating to
     * {@link NodeExtension#validateJoinRequest(JoinMessage)}
     */
    private boolean validateJoinRequest(JoinRequest joinRequest, Address target) {
        if (clusterService.isMaster()) {
            try {
                node.getNodeExtension().validateJoinRequest(joinRequest);
            } catch (Exception e) {
                logger.warning(e.getMessage());
                nodeEngine.getOperationService().send(new BeforeJoinCheckFailureOp(e.getMessage()), target);
                return false;
            }
        }
        return true;
    }

    /**
     * Start processing the join request. This method is executed by the master node. In the case that there hasn't been any
     * previous join requests from the {@code memberInfo}'s address the master will first respond by sending the master answer.
     *
     * Also, during the first {@link ClusterProperty#MAX_WAIT_SECONDS_BEFORE_JOIN} period since the master received the first
     * join request from any node, the master will always wait for {@link ClusterProperty#WAIT_SECONDS_BEFORE_JOIN} before
     * allowing any join request to proceed. This means that in the initial period from receiving the first ever join request,
     * every new join request from a different address will prolong the wait time. After the initial period, join requests
     * will get processed as they arrive for the first time.
     *
     * @param memberInfo the joining member info
     */
    private void startJoinRequest(MemberInfo memberInfo) {
        long now = Clock.currentTimeMillis();
        if (logger.isFineEnabled()) {
            String timeToStart = (timeToStartJoin > 0 ? ", timeToStart: " + (timeToStartJoin - now) : "");
            logger.fine(format("Handling join from %s, joinInProgress: %b%s", memberInfo.getAddress(),
                    joinInProgress, timeToStart));
        }

        if (firstJoinRequest == 0) {
            firstJoinRequest = now;
        }

        final MemberInfo existing = joiningMembers.put(memberInfo.getAddress(), memberInfo);
        if (existing == null) {
            sendMasterAnswer(memberInfo.getAddress());
            if (now - firstJoinRequest < maxWaitMillisBeforeJoin) {
                timeToStartJoin = now + waitMillisBeforeJoin;
            }
        } else if (!existing.getUuid().equals(memberInfo.getUuid())) {
            logger.warning("Received a new join request from " + memberInfo.getAddress()
                    + " with a new UUID " + memberInfo.getUuid()
                    + ". Previous UUID was " + existing.getUuid());
        }
        if (now >= timeToStartJoin) {
            startJoin();
        }
    }

    /**
     * Send join request to {@code toAddress}.
     *
     * @param toAddress       the currently known master address.
     * @return {@code true} if join request was sent successfully, otherwise {@code false}.
     */
    public boolean sendJoinRequest(Address toAddress) {
        if (toAddress == null) {
            toAddress = clusterService.getMasterAddress();
        }
        JoinRequestOp joinRequest = new JoinRequestOp(node.createJoinRequest(toAddress));
        return nodeEngine.getOperationService().send(joinRequest, toAddress);
    }

    public boolean setThisMemberAsMaster() {
        clusterServiceLock.lock();
        try {
            if (clusterService.isJoined()) {
                logger.warning("Cannot set as master because node is already joined!");
                return false;
            }

            logger.finest("This node is being set as the master");
            Address thisAddress = node.getThisAddress();
            MemberVersion version = node.getVersion();

            clusterService.setMasterAddress(thisAddress);

            if (clusterService.getClusterVersion().isUnknown()) {
                clusterService.getClusterStateManager().setClusterVersion(version.asVersion());
            }

            clusterService.getClusterClock().setClusterStartTime(Clock.currentTimeMillis());
            clusterService.setClusterId(UuidUtil.newUnsecureUUID());
            clusterService.getMembershipManager().setLocalMemberListJoinVersion(SINGLETON_MEMBER_LIST_VERSION);
            clusterService.setJoined(true);

            return true;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    /**
     * Set master address, if required.
     *
     * @param masterAddress address of cluster's master, as provided in {@link MasterResponseOp}
     * @param callerAddress address of node that sent the {@link MasterResponseOp}
     * @see MasterResponseOp
     */
    public void handleMasterResponse(Address masterAddress, Address callerAddress) {
        clusterServiceLock.lock();
        try {
            if (logger.isFineEnabled()) {
                logger.fine(format("Handling master response %s from %s", masterAddress, callerAddress));
            }

            if (clusterService.isJoined()) {
                if (logger.isFineEnabled()) {
                    logger.fine(format("Ignoring master response %s from %s, this node is already joined",
                            masterAddress, callerAddress));
                }
                return;
            }

            if (node.getThisAddress().equals(masterAddress)) {
                logger.warning("Received my address as master address from " + callerAddress);
                return;
            }

            Address currentMaster = clusterService.getMasterAddress();
            if (currentMaster == null || currentMaster.equals(masterAddress)) {
                setMasterAndJoin(masterAddress);
                return;
            }

            if (currentMaster.equals(callerAddress)) {
                logger.warning(format("Setting master to %s since %s says it is not master anymore", masterAddress,
                        currentMaster));
                setMasterAndJoin(masterAddress);
                return;
            }

            Connection conn = node.getServer().getConnectionManager(MEMBER).get(currentMaster);
            if (conn != null && conn.isAlive()) {
                logger.info(format("Ignoring master response %s from %s since this node has an active master %s",
                        masterAddress, callerAddress, currentMaster));
                sendJoinRequest(currentMaster);
            } else {
                logger.warning(format("Ambiguous master response! Received master response %s from %s. "
                                + "This node has a master %s, but does not have an active connection to it. "
                                + "Master field will be unset now.",
                        masterAddress, callerAddress, currentMaster));
                clusterService.setMasterAddress(null);
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void setMasterAndJoin(Address masterAddress) {
        clusterService.setMasterAddress(masterAddress);
        node.getServer().getConnectionManager(MEMBER).getOrConnect(masterAddress);
        if (!sendJoinRequest(masterAddress)) {
            logger.warning("Could not create connection to possible master " + masterAddress);
        }
    }

    /**
     * Send a {@link WhoisMasterOp} to designated address.
     *
     * @param toAddress the address to which the operation will be sent.
     * @return {@code true} if the operation was sent, otherwise {@code false}.
     */
    public boolean sendMasterQuestion(Address toAddress) {
        checkNotNull(toAddress, "No endpoint is specified!");

        BuildInfo buildInfo = node.getBuildInfo();
        Address thisAddress = node.getThisAddress();
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildInfo.getBuildNumber(), node.getVersion(),
                thisAddress, clusterService.getThisUuid(), node.isLiteMember(), node.createConfigCheck());
        return nodeEngine.getOperationService().send(new WhoisMasterOp(joinMessage), toAddress);
    }

    /**
     * Respond to a {@link WhoisMasterOp}.
     *
     * @param joinMessage the {@code JoinMessage} from the request.
     * @param connection  the connection to operation caller, to which response will be sent.
     * @see WhoisMasterOp
     */
    public void answerWhoisMasterQuestion(JoinMessage joinMessage, ServerConnection connection) {
        if (!ensureValidConfiguration(joinMessage)) {
            return;
        }

        if (clusterService.isJoined()) {
            if (!checkIfJoinRequestFromAnExistingMember(joinMessage, connection)) {
                sendMasterAnswer(joinMessage.getAddress());
            }
        } else {
            if (logger.isFineEnabled()) {
                logger.fine(format("Received a master question from %s,"
                        + " but this node is not master itself or doesn't have a master yet!", joinMessage.getAddress()));
            }
        }
    }

    /**
     * Respond to a join request by sending the master address in a {@link MasterResponseOp}. This happens when current node
     * receives a join request but is not the cluster's master.
     *
     * @param target the node receiving the master answer
     */
    private void sendMasterAnswer(Address target) {
        Address masterAddress = clusterService.getMasterAddress();
        if (masterAddress == null) {
            logger.info(format("Cannot send master answer to %s since master node is not known yet", target));
            return;
        }

        if (masterAddress.equals(node.getThisAddress())
                && node.getNodeExtension().getInternalHotRestartService()
                    .isMemberExcluded(masterAddress, clusterService.getThisUuid())) {
            // I already know that I will do a force-start, so I will not allow target to join me
            logger.info("Cannot send master answer because " + target + " should not join to this master node.");
            return;
        }

        if (masterAddress.equals(target)) {
            logger.fine("Cannot send master answer to " + target + " since it is the known master");
            return;
        }

        MasterResponseOp op = new MasterResponseOp(masterAddress);
        nodeEngine.getOperationService().send(op, target);
    }

    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    private boolean checkIfJoinRequestFromAnExistingMember(JoinMessage joinMessage, ServerConnection connection) {
        Address targetAddress = joinMessage.getAddress();
        MemberImpl member = clusterService.getMember(targetAddress);
        if (member == null) {
            return checkIfUsingAnExistingMemberUuid(joinMessage);
        }

        if (joinMessage.getUuid().equals(member.getUuid())) {
            sendMasterAnswer(targetAddress);

            if (clusterService.isMaster() && !isMastershipClaimInProgress()) {
                if (logger.isFineEnabled()) {
                    logger.fine(format("Ignoring join request, member already exists: %s", joinMessage));
                }

                // send members update back to node trying to join again...
                boolean deferPartitionProcessing = isMemberRestartingWithPersistence(member.getAttributes());
                OnJoinOp preJoinOp = preparePreJoinOps();
                OnJoinOp postJoinOp = preparePostJoinOp();
                PartitionRuntimeState partitionRuntimeState = node.getPartitionService().createPartitionState();

                Operation op = new FinalizeJoinOp(member.getUuid(),
                        clusterService.getMembershipManager().getMembersView(), preJoinOp, postJoinOp,
                        clusterClock.getClusterTime(), clusterService.getClusterId(),
                        clusterClock.getClusterStartTime(), clusterStateManager.getState(),
                        clusterService.getClusterVersion(), partitionRuntimeState, deferPartitionProcessing);
                op.setCallerUuid(clusterService.getThisUuid());
                invokeClusterOp(op, targetAddress);
            }
            return true;
        }

        // If I am the master, I will just suspect from the target. If it sends a new join request, it will be processed.
        // If I am not the current master, I can turn into the new master and start the claim process
        // after I suspect from the target.
        if (clusterService.isMaster() || targetAddress.equals(clusterService.getMasterAddress())) {
            String msg = format("New join request has been received from an existing endpoint %s."
                    + " Removing old member and processing join request...", member);
            logger.warning(msg);

            clusterService.suspectMember(member, msg, false);
            ServerConnection existing = node.getServer().getConnectionManager(MEMBER).get(targetAddress);
            if (existing != connection) {
                if (existing != null) {
                    existing.close(msg, null);
                }
                node.getServer().getConnectionManager(MEMBER).register(targetAddress, joinMessage.getUuid(), connection);
            }
        }
        return true;
    }

    /** check if member is joining with persistence enabled */
    private boolean isMemberRestartingWithPersistence(Map<String, String> attributes) {
        return attributes.get(PERSISTENCE_ENABLED_ATTRIBUTE) != null
                && attributes.get(PERSISTENCE_ENABLED_ATTRIBUTE).equals("true");
    }

    private boolean isMemberRejoining(MemberMap previousMembersMap, Address address, UUID memberUuid) {
        // may be already detected as crashed member
        return (hasMemberLeft(memberUuid)
                // or it is still in member list because connection timeout hasn't been reached yet
                || previousMembersMap.contains(memberUuid)
                // or it is a known missing member
                || clusterService.getMembershipManager().isMissingMember(address, memberUuid))
                && (node.getPartitionService().getLeftMemberSnapshot(memberUuid) != null);
    }

    private boolean checkIfUsingAnExistingMemberUuid(JoinMessage joinMessage) {
        Member member = clusterService.getMember(joinMessage.getUuid());
        Address target = joinMessage.getAddress();
        if (member != null && !member.getAddress().equals(joinMessage.getAddress())) {
            if (clusterService.isMaster() && !isMastershipClaimInProgress()) {
                String message = "There's already an existing member " + member + " with the same UUID. "
                        + target + " is not allowed to join.";
                logger.warning(message);
            } else {
                sendMasterAnswer(target);
            }
            return true;
        }
        return false;
    }

    void setMastershipClaimInProgress() {
        clusterServiceLock.lock();
        try {
            joinInProgress = true;
            joiningMembers.clear();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    /**
     * Starts join process on master member.
     */
    private void startJoin() {
        logger.fine("Starting join...");
        clusterServiceLock.lock();
        try {
            InternalPartitionService partitionService = node.getPartitionService();
            boolean shouldTriggerRepartition = true;
            try {
                joinInProgress = true;

                // pause migrations until join, member-update and post-join operations are completed
                partitionService.pauseMigration();
                MemberMap memberMap = clusterService.getMembershipManager().getMemberMap();

                MembersView newMembersView = MembersView.cloneAdding(memberMap.toMembersView(), joiningMembers.values());

                long time = clusterClock.getClusterTime();

                // member list must be updated on master before preparation of pre-/post-join ops so other operations which have
                // to be executed on stable cluster can detect the member list version change and retry in case of topology change
                UUID thisUuid = clusterService.getThisUuid();
                if (!clusterService.updateMembers(newMembersView, node.getThisAddress(), thisUuid, thisUuid)) {
                    return;
                }

                // post join operations must be lock free, that means no locks at all:
                // no partition locks, no key-based locks, no service level locks!
                OnJoinOp preJoinOp = preparePreJoinOps();
                OnJoinOp postJoinOp = preparePostJoinOp();

                // this is the current partition assignment state, not taking into account the
                // currently joining members
                PartitionRuntimeState partitionRuntimeState = partitionService.createPartitionState();
                for (MemberInfo member : joiningMembers.values()) {
                    if (isMemberRestartingWithPersistence(member.getAttributes())
                        && isMemberRejoining(memberMap, member.getAddress(), member.getUuid())) {
                        logger.info(member + " is rejoining the cluster");
                        // do not trigger repartition immediately, wait for joining member to load hot-restart data
                        shouldTriggerRepartition = false;
                    }
                    long startTime = clusterClock.getClusterStartTime();
                    Operation op = new FinalizeJoinOp(member.getUuid(), newMembersView, preJoinOp, postJoinOp, time,
                            clusterService.getClusterId(), startTime, clusterStateManager.getState(),
                            clusterService.getClusterVersion(), partitionRuntimeState, !shouldTriggerRepartition);
                    op.setCallerUuid(thisUuid);
                    invokeClusterOp(op, member.getAddress());
                }
                for (MemberImpl member : memberMap.getMembers()) {
                    if (member.localMember() || joiningMembers.containsKey(member.getAddress())) {
                        continue;
                    }
                    Operation op = new MembersUpdateOp(member.getUuid(), newMembersView, time, partitionRuntimeState, true);
                    op.setCallerUuid(thisUuid);
                    invokeClusterOp(op, member.getAddress());
                }

            } finally {
                reset();
                if (shouldTriggerRepartition) {
                    partitionService.resumeMigration();
                }
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private OnJoinOp preparePostJoinOp() {
        Collection<Operation> postJoinOps = nodeEngine.getPostJoinOperations();
        return (postJoinOps != null && !postJoinOps.isEmpty()) ? new OnJoinOp(postJoinOps) : null;
    }

    private OnJoinOp preparePreJoinOps() {
        Collection<Operation> preJoinOps = nodeEngine.getPreJoinOperations();
        return (preJoinOps != null && !preJoinOps.isEmpty()) ? new OnJoinOp(preJoinOps) : null;
    }

    private Future invokeClusterOp(Operation op, Address target) {
        return nodeEngine.getOperationService()
                .createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME, op, target)
                .setTryCount(CLUSTER_OPERATION_RETRY_COUNT).invoke();
    }

    @SuppressWarnings({"checkstyle:returncount", "checkstyle:npathcomplexity"})
    public SplitBrainJoinMessage.SplitBrainMergeCheckResult shouldMerge(SplitBrainJoinMessage joinMessage) {
        if (joinMessage == null) {
            return CANNOT_MERGE;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Checking if we should merge to: " + joinMessage);
        }

        if (!checkValidSplitBrainJoinMessage(joinMessage)) {
            return CANNOT_MERGE;
        }

        if (!checkCompatibleSplitBrainJoinMessage(joinMessage)) {
            return CANNOT_MERGE;
        }

        if (!checkMergeTargetIsNotMember(joinMessage)) {
            return CANNOT_MERGE;
        }

        if (!checkClusterStateAllowsJoinBeforeMerge(joinMessage)) {
            return CANNOT_MERGE;
        }

        if (!checkMembershipIntersectionSetEmpty(joinMessage)) {
            return CANNOT_MERGE;
        }

        int targetDataMemberCount = joinMessage.getDataMemberCount();
        int currentDataMemberCount = clusterService.getSize(DATA_MEMBER_SELECTOR);

        if (targetDataMemberCount > currentDataMemberCount) {
            logger.info("We should merge to " + joinMessage.getAddress()
                    + " because their data member count is bigger than ours ["
                    + (targetDataMemberCount + " > " + currentDataMemberCount) + ']');
            return LOCAL_NODE_SHOULD_MERGE;
        }

        if (targetDataMemberCount < currentDataMemberCount) {
            logger.info(joinMessage.getAddress() + " should merge to us "
                    + "because our data member count is bigger than theirs ["
                    + (currentDataMemberCount + " > " + targetDataMemberCount) + ']');
            return REMOTE_NODE_SHOULD_MERGE;
        }

        // targetDataMemberCount == currentDataMemberCount
        if (shouldMergeTo(node.getThisAddress(), joinMessage.getAddress())) {
            logger.info("We should merge to " + joinMessage.getAddress()
                    + ", both have the same data member count: " + currentDataMemberCount);
            return LOCAL_NODE_SHOULD_MERGE;
        }

        logger.info(joinMessage.getAddress() + " should merge to us"
                + ", both have the same data member count: " + currentDataMemberCount);
        return REMOTE_NODE_SHOULD_MERGE;
    }

    private boolean checkValidSplitBrainJoinMessage(SplitBrainJoinMessage joinMessage) {
        try {
            if (!validateJoinMessage(joinMessage)) {
                logger.fine("Cannot process split brain merge message from " + joinMessage.getAddress()
                        + ", since join-message could not be validated.");
                return false;
            }
        } catch (Exception e) {
            logger.fine("failure during validating join message", e);
            return false;
        }
        return true;
    }

    private boolean checkCompatibleSplitBrainJoinMessage(SplitBrainJoinMessage joinMessage) {
        Version clusterVersion = clusterService.getClusterVersion();
        if (!clusterVersion.isEqualTo(joinMessage.getClusterVersion())) {
            if (logger.isFineEnabled()) {
                logger.fine("Should not merge to " + joinMessage.getAddress() + " because other cluster version is "
                        + joinMessage.getClusterVersion() + " while this cluster version is "
                        + clusterVersion);
            }
            return false;
        }
        return true;
    }

    private boolean checkMergeTargetIsNotMember(SplitBrainJoinMessage joinMessage) {
        if (clusterService.getMember(joinMessage.getAddress()) != null) {
            if (logger.isFineEnabled()) {
                logger.fine("Should not merge to " + joinMessage.getAddress()
                        + ", because it is already member of this cluster.");
            }
            return false;
        }
        return true;
    }

    private boolean checkClusterStateAllowsJoinBeforeMerge(SplitBrainJoinMessage joinMessage) {
        ClusterState clusterState = clusterService.getClusterState();
        if (!clusterState.isJoinAllowed()) {
            if (logger.isFineEnabled()) {
                logger.fine("Should not merge to " + joinMessage.getAddress() + ", because this cluster is in "
                        + clusterState + " state.");
            }
            return false;
        }
        return true;
    }

    private boolean checkMembershipIntersectionSetEmpty(SplitBrainJoinMessage joinMessage) {
        Collection<Address> targetMemberAddresses = joinMessage.getMemberAddresses();
        Address joinMessageAddress = joinMessage.getAddress();
        if (targetMemberAddresses.contains(node.getThisAddress())) {
            // Join request is coming from master of the split and it thinks that I am its member.
            // This is partial split case and we want to convert it to a full split.
            // So it should remove me from its cluster.
            MembersViewMetadata membersViewMetadata = new MembersViewMetadata(joinMessageAddress, joinMessage.getUuid(),
                    joinMessageAddress, joinMessage.getMemberListVersion());
            clusterService.sendExplicitSuspicion(membersViewMetadata);
            logger.info(node.getThisAddress() + " CANNOT merge to " + joinMessageAddress
                    + ", because it thinks this-node as its member.");
            return false;
        }

        for (Address address : clusterService.getMemberAddresses()) {
            if (targetMemberAddresses.contains(address)) {
                logger.info(node.getThisAddress() + " CANNOT merge to " + joinMessageAddress
                        + ", because it thinks " + address + " is its member. "
                        + "But " + address + " is member of this cluster.");
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether this address should merge to target address and called when two sides are equal on all aspects.
     * This is a pure function that must produce always the same output when called with the same parameters.
     * This logic should not be changed, otherwise compatibility will be broken.
     *
     * @param thisAddress this address
     * @param targetAddress target address
     * @return true if this address should merge to target, false otherwise
     */
    private boolean shouldMergeTo(Address thisAddress, Address targetAddress) {
        String thisAddressStr = "[" + thisAddress.getHost() + "]:" + thisAddress.getPort();
        String targetAddressStr = "[" + targetAddress.getHost() + "]:" + targetAddress.getPort();

        if (thisAddressStr.equals(targetAddressStr)) {
            throw new IllegalArgumentException("Addresses must be different! This: "
                    + thisAddress + ", Target: " + targetAddress);
        }

        // Since strings are guaranteed to be different, result will always be non-zero.
        int result = thisAddressStr.compareTo(targetAddressStr);
        return result > 0;
    }

    void reset() {
        clusterServiceLock.lock();
        try {
            joinInProgress = false;
            joiningMembers.clear();
            timeToStartJoin = Clock.currentTimeMillis() + waitMillisBeforeJoin;
            firstJoinRequest = 0;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void removeJoin(Address address) {
        joiningMembers.remove(address);
    }

    void addLeftMember(Member member) {
        leftMembersUuids.put(member.getUuid(), Clock.currentTimeMillis());
    }

    public boolean hasMemberLeft(UUID memberUuid) {
        return leftMembersUuids.containsKey(memberUuid);
    }

    public void removeLeftMember(UUID memberUuid) {
        leftMembersUuids.remove(memberUuid);
    }
}
