/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Member;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.AuthenticationFailureOperation;
import com.hazelcast.internal.cluster.impl.operations.BeforeJoinCheckFailureOperation;
import com.hazelcast.internal.cluster.impl.operations.ConfigMismatchOperation;
import com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOperation;
import com.hazelcast.internal.cluster.impl.operations.GroupMismatchOperation;
import com.hazelcast.internal.cluster.impl.operations.JoinRequestOperation;
import com.hazelcast.internal.cluster.impl.operations.MasterDiscoveryOperation;
import com.hazelcast.internal.cluster.impl.operations.MemberInfoUpdateOperation;
import com.hazelcast.internal.cluster.impl.operations.PostJoinOperation;
import com.hazelcast.internal.cluster.impl.operations.SetMasterOperation;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.Clock;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.version.MemberVersion;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.createMemberInfoList;
import static com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOperation.FINALIZE_JOIN_MAX_TIMEOUT;
import static com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOperation.FINALIZE_JOIN_TIMEOUT_FACTOR;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * ClusterJoinManager manages member join process.
 * <p/>
 * If this node is not master,
 * then it will answer with sending master node's address to a join request.
 * <p/>
 * If this is master node, it will handle join request and notify all other members
 * about newly joined member.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class ClusterJoinManager {

    private static final int CLUSTER_OPERATION_RETRY_COUNT = 100;

    private final ILogger logger;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final Lock clusterServiceLock;
    private final ClusterClockImpl clusterClock;
    private final ClusterStateManager clusterStateManager;

    private final Map<Address, MemberInfo> joiningMembers = new LinkedHashMap<Address, MemberInfo>();
    private final long maxWaitMillisBeforeJoin;
    private final long waitMillisBeforeJoin;
    private final FutureUtil.ExceptionHandler whileFinalizeJoinsExceptionHandler;

    private long firstJoinRequest;
    private long timeToStartJoin;
    private boolean joinInProgress;

    public ClusterJoinManager(Node node, ClusterServiceImpl clusterService, Lock clusterServiceLock) {
        this.node = node;
        this.clusterService = clusterService;
        this.clusterServiceLock = clusterServiceLock;
        this.nodeEngine = clusterService.getNodeEngine();

        logger = node.getLogger(getClass());
        clusterStateManager = clusterService.getClusterStateManager();
        clusterClock = clusterService.getClusterClock();

        maxWaitMillisBeforeJoin = node.getProperties().getMillis(GroupProperty.MAX_WAIT_SECONDS_BEFORE_JOIN);
        waitMillisBeforeJoin = node.getProperties().getMillis(GroupProperty.WAIT_SECONDS_BEFORE_JOIN);
        whileFinalizeJoinsExceptionHandler = logAllExceptions(logger, "While waiting finalize join calls...",
                Level.WARNING);
    }

    public boolean isJoinInProgress() {
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

    /**
     * Handle a {@link JoinRequestOperation}. If this node is not master, reply with a {@link SetMasterOperation} to let the
     * joining node know the current master. Otherwise, if no other join is in progress, execute the {@link JoinRequest}
     *
     * @param joinRequest the join request
     * @param connection the connection to the joining node
     * @see JoinRequestOperation
     */
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
            if (logger.isFineEnabled()) {
                logger.fine(format("Join is in progress, cannot handle join request from %s at the moment", target));
            }
            return;
        }

        executeJoinRequest(joinRequest, connection);
    }

    private boolean ensureNodeIsReady() {
        if (node.joined() && node.isRunning()) {
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
     * @throws Exception in case any exception occurred while checking compatibilty
     * @see ConfigCheck
     */
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

    /**
     * Executed by a master node to process the {@link JoinRequest} sent by a node attempting to join the cluster.
     *
     * @param joinRequest the join request from a node attempting to join
     * @param connection  the connection of this node to the joining node
     */
    private void executeJoinRequest(JoinRequest joinRequest, Connection connection) {
        clusterServiceLock.lock();
        try {
            if (checkJoinRequest(joinRequest, connection)) {
                return;
            }

            if (!authenticate(joinRequest)) {
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
    private boolean checkJoinRequest(JoinRequest joinRequest, Connection connection) {
        if (checkIfJoinRequestFromAnExistingMember(joinRequest, connection)) {
            return true;
        }

        final InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
        Address target = joinRequest.getAddress();
        String targetUuid = joinRequest.getUuid();

        if (hotRestartService.isMemberExcluded(target, targetUuid)) {
            logger.fine("cannot join " + target + " because it is excluded in cluster start.");
            hotRestartService.notifyExcludedMember(target);
            return true;
        }

        if (checkClusterStateBeforeJoin(target, targetUuid)) {
            return true;
        }

        if (joinRequest.getExcludedMemberUuids().contains(node.getThisUuid())) {
            logger.warning("cannot join " + target + " since this node is excluded in its list...");
            hotRestartService.handleExcludedMemberUuids(target, joinRequest.getExcludedMemberUuids());
            return true;
        }

        if (!node.getPartitionService().isMemberAllowedToJoin(target)) {
            logger.warning(target + " not allowed to join right now, it seems restarted.");
            return true;
        }

        return false;
    }

    private boolean checkClusterStateBeforeJoin(Address target, String uuid) {
        ClusterState state = clusterStateManager.getState();
        if (state == ClusterState.IN_TRANSITION) {
            logger.warning("Cluster state is in transition process. Join is not allowed until "
                    + "transaction is completed -> "
                    + clusterStateManager.stateToString());
            return true;
        }

        if (state == ClusterState.ACTIVE) {
            return false;
        }

        if (clusterService.isMemberRemovedWhileClusterIsNotActive(target)) {
            MemberImpl memberRemovedWhileClusterIsNotActive =
                    clusterService.getMemberRemovedWhileClusterIsNotActive(uuid);

            if (memberRemovedWhileClusterIsNotActive != null
                    && !target.equals(memberRemovedWhileClusterIsNotActive.getAddress())) {

                logger.warning("Uuid " + uuid + " was being used by " + memberRemovedWhileClusterIsNotActive
                        + " before. " + target + " is not allowed to join with a uuid which belongs to"
                        + " a known passive member.");

                return true;
            }

            return false;
        }

        if (clusterService.isMemberRemovedWhileClusterIsNotActive(uuid)) {
            return false;
        }

        if (node.getNodeExtension().isStartCompleted()) {
            String message = "Cluster state either is locked or doesn't allow new members to join -> "
                    + clusterStateManager.stateToString();
            logger.warning(message);

            OperationService operationService = nodeEngine.getOperationService();
            BeforeJoinCheckFailureOperation op = new BeforeJoinCheckFailureOperation(message);
            operationService.send(op, target);
        } else {
            String message = "Cluster state either is locked or doesn't allow new members to join -> "
                    + clusterStateManager.stateToString() + ". Silently ignored join request of " + target
                    + " because start not completed.";
            logger.warning(message);
        }
        return true;
    }

    private boolean authenticate(JoinRequest joinRequest) {
        if (!joiningMembers.containsKey(joinRequest.getAddress())) {
            try {
                secureLogin(joinRequest);
            } catch (Exception e) {
                ILogger securityLogger = node.loggingService.getLogger("com.hazelcast.security");
                nodeEngine.getOperationService().send(new AuthenticationFailureOperation(), joinRequest.getAddress());
                securityLogger.severe(e);
                return false;
            }
        }
        return true;
    }

    private void secureLogin(JoinRequest joinRequest) {
        if (node.securityContext != null) {
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

    /**
     * Invoked from master node while executing a join request to validate it, delegating to
     * {@link com.hazelcast.instance.NodeExtension#validateJoinRequest(JoinMessage)}
     */
    private boolean validateJoinRequest(JoinRequest joinRequest, Address target) {
        if (node.isMaster()) {
            try {
                node.getNodeExtension().validateJoinRequest(joinRequest);
            } catch (Exception e) {
                logger.warning(e.getMessage());
                nodeEngine.getOperationService().send(new BeforeJoinCheckFailureOperation(e.getMessage()), target);
                return false;
            }
        }
        return true;
    }

    /**
     * Start processing the join request. This method is executed by the master node. In the case that there hasn't been any
     * previous join requests from the {@code memberInfo}'s address the master will first respond by sending the master answer.
     *
     * Also, during the first {@link GroupProperty#MAX_WAIT_SECONDS_BEFORE_JOIN} period since the master received the first
     * join request from any node, the master will always wait for {@link GroupProperty#WAIT_SECONDS_BEFORE_JOIN} before
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
                    + " with a new uuid " + memberInfo.getUuid()
                    + ". Previous uuid was " + existing.getUuid());
        }
        if (now >= timeToStartJoin) {
            startJoin();
        }
    }

    /**
     * Send join request to {@code toAddress}.
     *
     * @param toAddress       the currently known master address.
     * @param withCredentials use cluster credentials
     * @return {@code true} if join request was sent successfully, otherwise {@code false}.
     */
    public boolean sendJoinRequest(Address toAddress, boolean withCredentials) {
        if (toAddress == null) {
            toAddress = node.getMasterAddress();
        }
        JoinRequestOperation joinRequest = new JoinRequestOperation(node.createJoinRequest(withCredentials));
        return nodeEngine.getOperationService().send(joinRequest, toAddress);
    }

    /**
     * Set master address, if required.
     *
     * @param masterAddress address of cluster's master, as provided in {@link SetMasterOperation}
     * @param callerAddress address of node that sent the {@link SetMasterOperation}
     * @see SetMasterOperation
     */
    public void setMaster(Address masterAddress, Address callerAddress) {
        if (node.joined()) {
            if (logger.isFineEnabled()) {
                logger.fine(format("Ignoring master response %s from %s, this node is already joined",
                        masterAddress, callerAddress));
            }
            return;
        }

        if (node.getThisAddress().equals(masterAddress)) {
            if (node.isMaster()) {
                logger.fine(format("Ignoring master response %s from %s, this node is already master",
                        masterAddress, callerAddress));
            } else {
                setAsMaster();
            }
            return;
        }

        handleMasterResponse(masterAddress, callerAddress);
    }

    public boolean setMasterAddress(final Address master) {
        clusterServiceLock.lock();
        try {
            if (node.joined()) {
                logger.warning("Cannot set master address to " + master
                        + " because node is already joined! Current master address: " + node.getMasterAddress());
                return false;
            }

            node.setMasterAddress(master);
            return true;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    public boolean setAsMaster() {
        clusterServiceLock.lock();
        try {
            if (node.joined()) {
                logger.warning("Cannot set as master because node is already joined!");
                return false;
            }

            logger.finest("This node is being set as the master");
            Address thisAddress = node.getThisAddress();
            MemberVersion version = node.getVersion();

            node.setMasterAddress(thisAddress);

            if (clusterService.getClusterVersion() == null) {
                clusterService.getClusterStateManager().setClusterVersion(version.asVersion());
            }

            clusterService.getClusterClock().setClusterStartTime(Clock.currentTimeMillis());
            clusterService.setClusterId(UuidUtil.createClusterUuid());
            node.setJoined();

            return true;
        } finally {
            clusterServiceLock.unlock();
        }

    }

    private void handleMasterResponse(Address masterAddress, Address callerAddress) {
        clusterServiceLock.lock();
        try {
            if (logger.isFineEnabled()) {
                logger.fine(format("Handling master response %s from %s", masterAddress, callerAddress));
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
                setMasterAddress(null);
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void setMasterAndJoin(Address masterAddress) {
        setMasterAddress(masterAddress);
        node.connectionManager.getOrConnect(masterAddress);
        if (!sendJoinRequest(masterAddress, true)) {
            logger.warning("Could not create connection to possible master " + masterAddress);
        }
    }

    /**
     * Send a {@link MasterDiscoveryOperation} to designated address.
     *
     * @param toAddress the address to which the operation will be sent.
     * @return {@code true} if the operation was sent, otherwise {@code false}.
     */
    public boolean sendMasterQuestion(Address toAddress) {
        checkNotNull(toAddress, "No endpoint is specified!");

        BuildInfo buildInfo = node.getBuildInfo();
        final Address thisAddress = node.getThisAddress();
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildInfo.getBuildNumber(), node.getVersion(),
                thisAddress, node.getThisUuid(), node.isLiteMember(), node.createConfigCheck());
        return nodeEngine.getOperationService().send(new MasterDiscoveryOperation(joinMessage), toAddress);
    }

    /**
     * Respond to a {@link MasterDiscoveryOperation}.
     *
     * @param joinMessage the {@code JoinMessage} from the request.
     * @param connection  the connection to operation caller, to which response will be sent.
     * @see MasterDiscoveryOperation
     */
    public void answerMasterQuestion(JoinMessage joinMessage, Connection connection) {
        if (!ensureValidConfiguration(joinMessage)) {
            return;
        }

        if (node.getMasterAddress() != null) {
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
     * Respond to a join request by sending the master address in a {@link SetMasterOperation}. This happens when current node
     * receives a join request but is not the cluster's master.
     *
     * @param target the node receiving the master answer
     */
    private void sendMasterAnswer(Address target) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            logger.info(format("Cannot send master answer to %s since master node is not known yet", target));
            return;
        }

        if (masterAddress.equals(node.getThisAddress())
                && node.getNodeExtension().getInternalHotRestartService().isMemberExcluded(masterAddress, node.getThisUuid())) {
            // I already know that I will do a force-start so I will not allow target to join me
            logger.info("Cannot send master answer because " + target + " should not join to this master node.");
            return;
        }

        SetMasterOperation op = new SetMasterOperation(masterAddress);
        nodeEngine.getOperationService().send(op, target);
    }

    private boolean checkIfJoinRequestFromAnExistingMember(JoinMessage joinMessage, Connection connection) {
        Address target = joinMessage.getAddress();
        MemberImpl member = clusterService.getMember(target);
        if (member == null) {
            return checkIfUsingAnExistingMemberUuid(joinMessage);
        }

        if (joinMessage.getUuid().equals(member.getUuid())) {
            sendMasterAnswer(target);

            if (node.isMaster()) {
                if (logger.isFineEnabled()) {
                    logger.fine(format("Ignoring join request, member already exists: %s", joinMessage));
                }

                // send members update back to node trying to join again...
                Operation[] postJoinOps = nodeEngine.getPostJoinOperations();
                boolean isPostJoinOperation = postJoinOps != null && postJoinOps.length > 0;
                PostJoinOperation postJoinOp = isPostJoinOperation ? new PostJoinOperation(postJoinOps) : null;
                PartitionRuntimeState partitionRuntimeState = node.getPartitionService().createPartitionState();

                List<MemberInfo> memberInfos = createMemberInfoList(clusterService.getMemberImpls());
                Operation operation = new FinalizeJoinOperation(member.getUuid(), memberInfos, postJoinOp,
                        clusterClock.getClusterTime(), clusterService.getClusterId(), clusterClock.getClusterStartTime(),
                        clusterStateManager.getState(), clusterService.getClusterVersion(), partitionRuntimeState, false);
                nodeEngine.getOperationService().send(operation, target);
            }
            return true;
        }

        // remove old member and process join request:
        // - if this node is master OR
        // - if requesting address is equal to master node's address, that means the master node somehow disconnected
        //   and wants to join back, so drop old member and process join request if this node becomes master
        if (node.isMaster() || target.equals(node.getMasterAddress())) {
            String msg = format("New join request has been received from an existing endpoint %s."
                    + " Removing old member and processing join request...", member);
            logger.warning(msg);

            clusterService.doRemoveAddress(target, msg, false);
            Connection existing = node.connectionManager.getConnection(target);
            if (existing != connection) {
                if (existing != null) {
                    existing.close(msg, null);
                }
                node.connectionManager.registerConnection(target, connection);
            }
            return false;
        }
        return true;
    }

    private boolean checkIfUsingAnExistingMemberUuid(JoinMessage joinMessage) {
        Member member = clusterService.getMember(joinMessage.getUuid());
        Address target = joinMessage.getAddress();
        if (member != null && !member.getAddress().equals(joinMessage.getAddress())) {
            if (node.isMaster()) {
                String message = "There's already an existing member " + member + " with the same UUID. "
                        + target + " is not allowed to join.";
                logger.warning(message);
                OperationService operationService = nodeEngine.getOperationService();
                operationService.send(new BeforeJoinCheckFailureOperation(message), target);
            } else {
                sendMasterAnswer(target);
            }
            return true;
        }
        return false;
    }

    private void startJoin() {
        logger.fine("Starting join...");
        clusterServiceLock.lock();
        try {
            InternalPartitionService partitionService = node.getPartitionService();
            try {
                joinInProgress = true;

                // pause migrations until join, member-update and post-join operations are completed
                partitionService.pauseMigration();
                Collection<MemberImpl> members = clusterService.getMemberImpls();
                Collection<MemberInfo> memberInfos = createMemberInfoList(members);
                for (MemberInfo memberJoining : joiningMembers.values()) {
                    memberInfos.add(memberJoining);
                }
                long time = clusterClock.getClusterTime();

                // post join operations must be lock free, that means no locks at all:
                // no partition locks, no key-based locks, no service level locks!
                Operation[] postJoinOps = nodeEngine.getPostJoinOperations();
                boolean createPostJoinOperation = (postJoinOps != null && postJoinOps.length > 0);
                PostJoinOperation postJoinOp = (createPostJoinOperation ? new PostJoinOperation(postJoinOps) : null);

                clusterService.updateMembers(memberInfos, node.getThisAddress());

                int count = members.size() - 1 + joiningMembers.size();
                List<Future> calls = new ArrayList<Future>(count);
                PartitionRuntimeState partitionRuntimeState = partitionService.createPartitionState();
                for (MemberInfo member : joiningMembers.values()) {
                    long startTime = clusterClock.getClusterStartTime();
                    Operation finalizeJoinOperation = new FinalizeJoinOperation(member.getUuid(), memberInfos, postJoinOp, time,
                            clusterService.getClusterId(), startTime,
                            clusterStateManager.getState(), clusterService.getClusterVersion(), partitionRuntimeState);
                    calls.add(invokeClusterOperation(finalizeJoinOperation, member.getAddress()));
                }
                for (MemberImpl member : members) {
                    if (member.localMember() || joiningMembers.containsKey(member.getAddress())) {
                        continue;
                    }
                    Operation memberInfoUpdateOperation = new MemberInfoUpdateOperation(member.getUuid(), memberInfos, time,
                            partitionRuntimeState, true);
                    calls.add(invokeClusterOperation(memberInfoUpdateOperation, member.getAddress()));
                }

                int timeout = Math.min(calls.size() * FINALIZE_JOIN_TIMEOUT_FACTOR, FINALIZE_JOIN_MAX_TIMEOUT);
                waitWithDeadline(calls, timeout, TimeUnit.SECONDS, whileFinalizeJoinsExceptionHandler);
            } finally {
                reset();
                partitionService.resumeMigration();
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private Future invokeClusterOperation(Operation op, Address target) {
        return nodeEngine.getOperationService()
                .createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME, op, target)
                .setTryCount(CLUSTER_OPERATION_RETRY_COUNT).invoke();
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
}
