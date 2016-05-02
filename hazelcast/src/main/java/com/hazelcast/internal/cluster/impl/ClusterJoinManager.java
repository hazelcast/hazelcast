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

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
public class ClusterJoinManager {

    private static final int CLUSTER_OPERATION_RETRY_COUNT = 100;

    private final ILogger logger;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final Lock clusterServiceLock;
    private final ClusterClockImpl clusterClock;
    private final ClusterStateManager clusterStateManager;

    private final Set<MemberInfo> setJoins = new LinkedHashSet<MemberInfo>(100);
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
            return joinInProgress || !setJoins.isEmpty();
        } finally {
            clusterServiceLock.unlock();
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
        if (node.joined() && node.isRunning()) {
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

    private boolean isValidJoinMessage(JoinMessage joinMessage) {
        try {
            return validateJoinMessage(joinMessage);
        } catch (ConfigMismatchException e) {
            throw e;
        } catch (Exception e) {
            return false;
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

    private void executeJoinRequest(JoinRequest joinRequest, Connection connection, Address target) {
        clusterServiceLock.lock();
        try {
            if (checkIfJoinRequestFromAnExistingMember(joinRequest, connection)) {
                return;
            }

            if (checkClusterStateBeforeJoin(target)) {
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

            if (!validateJoinRequest(target)) {
                return;
            }

            startJoinRequest(target, now, memberInfo);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private boolean checkClusterStateBeforeJoin(Address target) {
        if (clusterStateManager.getState() == ClusterState.IN_TRANSITION) {
            String message = "Cluster state either is in transition process. Join is not allowed for now -> "
                    + clusterStateManager.stateToString();
            logger.warning(message);
            OperationService operationService = nodeEngine.getOperationService();
            operationService.send(new BeforeJoinCheckFailureOperation(message), target);
            return true;
        } else if (clusterStateManager.getState() != ClusterState.ACTIVE) {
            if (!clusterService.isMemberRemovedWhileClusterIsNotActive(target)) {
                String message = "Cluster state either is locked or doesn't allow new members to join -> "
                        + clusterStateManager.stateToString();
                logger.warning(message);
                OperationService operationService = nodeEngine.getOperationService();
                operationService.send(new BeforeJoinCheckFailureOperation(message), target);
                return true;
            }
        }
        return false;
    }

    private MemberInfo getMemberInfo(JoinRequest joinRequest, Address target) {
        MemberInfo memberInfo = joinRequest.toMemberInfo();
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

    private boolean validateJoinRequest(Address target) {
        if (node.isMaster()) {
            try {
                node.getNodeExtension().validateJoinRequest();
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
        if (now >= timeToStartJoin) {
            startJoin();
        }
    }

    public boolean sendJoinRequest(Address toAddress, boolean withCredentials) {
        if (toAddress == null) {
            toAddress = node.getMasterAddress();
        }
        JoinRequestOperation joinRequest = new JoinRequestOperation(node.createJoinRequest(withCredentials));
        return nodeEngine.getOperationService().send(joinRequest, toAddress);
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
        clusterServiceLock.lock();
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
            clusterServiceLock.unlock();
        }
    }

    private void setMasterAndJoin(Address masterAddress) {
        node.setMasterAddress(masterAddress);
        node.connectionManager.getOrConnect(masterAddress);
        if (!sendJoinRequest(masterAddress, true)) {
            logger.warning("Could not create connection to possible master " + masterAddress);
        }
    }

    public boolean sendMasterQuestion(Address toAddress) {
        checkNotNull(toAddress, "No endpoint is specified!");

        BuildInfo buildInfo = node.getBuildInfo();
        final Address thisAddress = node.getThisAddress();
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildInfo.getBuildNumber(), thisAddress,
                node.getLocalMember().getUuid(), node.isLiteMember(), node.createConfigCheck());
        return nodeEngine.getOperationService().send(new MasterDiscoveryOperation(joinMessage), toAddress);
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

    private void sendMasterAnswer(Address target) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            logger.info(format("Cannot send master answer to %s since master node is not known yet", target));
            return;
        }
        SetMasterOperation op = new SetMasterOperation(masterAddress);
        nodeEngine.getOperationService().send(op, target);
    }

    private boolean checkIfJoinRequestFromAnExistingMember(JoinMessage joinMessage, Connection connection) {
        MemberImpl member = clusterService.getMember(joinMessage.getAddress());
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
                PartitionRuntimeState partitionRuntimeState = node.getPartitionService().createPartitionState();

                Operation operation = new FinalizeJoinOperation(createMemberInfoList(clusterService.getMemberImpls()),
                        postJoinOp, clusterClock.getClusterTime(), clusterStateManager.getState(),
                        partitionRuntimeState, false);
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

            clusterService.doRemoveAddress(target, false);
            Connection existing = node.connectionManager.getConnection(target);
            if (existing != connection) {
                node.connectionManager.destroyConnection(existing);
                node.connectionManager.registerConnection(target, connection);
            }
            return false;
        }
        return true;
    }

    private void startJoin() {
        logger.finest("Starting join...");
        clusterServiceLock.lock();
        try {
            try {
                joinInProgress = true;

                // pause migrations until join, member-update and post-join operations are completed
                node.getPartitionService().pauseMigration();
                Collection<MemberImpl> members = clusterService.getMemberImpls();
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
                PartitionRuntimeState partitionState = node.getPartitionService().createPartitionState();
                for (MemberInfo member : setJoins) {
                    long startTime = clusterClock.getClusterStartTime();
                    Operation joinOperation = new FinalizeJoinOperation(memberInfos, postJoinOp, time,
                            clusterService.getClusterId(), startTime,
                            clusterStateManager.getState(), partitionState);
                    calls.add(invokeClusterOperation(joinOperation, member.getAddress()));
                }
                for (MemberImpl member : members) {
                    if (!member.getAddress().equals(clusterService.getThisAddress())) {
                        Operation infoUpdateOperation = new MemberInfoUpdateOperation(memberInfos, time, true);
                        calls.add(invokeClusterOperation(infoUpdateOperation, member.getAddress()));
                    }
                }

                clusterService.updateMembers(memberInfos);
                int timeout = Math.min(calls.size() * FINALIZE_JOIN_TIMEOUT_FACTOR, FINALIZE_JOIN_MAX_TIMEOUT);
                waitWithDeadline(calls, timeout, TimeUnit.SECONDS, whileFinalizeJoinsExceptionHandler);
            } finally {
                node.getPartitionService().resumeMigration();
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
            setJoins.clear();
            timeToStartJoin = Clock.currentTimeMillis() + waitMillisBeforeJoin;
            firstJoinRequest = 0;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void removeJoin(MemberInfo memberInfo) {
        setJoins.remove(memberInfo);
    }
}
