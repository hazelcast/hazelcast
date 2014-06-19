/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.SystemLogService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.LoggingUtil.logIfFinestEnabled;

public abstract class AbstractJoiner implements Joiner {
    private static final int JOIN_TRY_COUNT = 5;
    private static final long BUSY_WAIT_MILLIS = 1000;
    private static final long REBOOT_WAIT_MILLIS = 10000;

    protected final Config config;
    protected final Node node;
    protected final ILogger logger;
    protected final SystemLogService systemLogService;
    private final AtomicLong joinStartTime = new AtomicLong(Clock.currentTimeMillis());
    private final AtomicInteger tryCount = new AtomicInteger(0);
    private volatile Address targetAddress;

    public AbstractJoiner(Node node) {
        this.node = node;
        this.systemLogService = node.getSystemLogService();
        this.logger = node.loggingService.getLogger(this.getClass().getName());
        this.config = node.config;
    }

    public abstract void doJoin(AtomicBoolean joined);

    @Override
    public void join(AtomicBoolean joined) {
        doJoin(joined);
        postJoin();
    }

    private void postJoin() {
        systemLogService.logJoin("PostJoin master: " + node.getMasterAddress() + ", isMaster: " + node.isMaster());
        if (!node.isActive()) {
            return;
        }
        if (tryCount.incrementAndGet() == JOIN_TRY_COUNT) {
            logger.warning("Join try count exceed limit, setting this node as master!");
            node.setAsMaster();
        }
        if (!node.isMaster()) {
            boolean allConnected = false;
            int checkCount = 0;
            final int maxJoinSeconds = node.getGroupProperties().MAX_JOIN_SECONDS.getInteger();
            final long maxJoinMillis = TimeUnit.SECONDS.toMillis(maxJoinSeconds);
            if (node.joined()) {
                systemLogService.logJoin("Waiting for all connections");
                while (checkCount++ < node.groupProperties.CONNECT_ALL_WAIT_SECONDS.getInteger() && !allConnected) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(BUSY_WAIT_MILLIS);
                    } catch (InterruptedException ignored) {
                        ignore(ignored);
                    }
                    Set<Member> members = node.getClusterService().getMembers();
                    allConnected = true;
                    for (Member member : members) {
                        MemberImpl memberImpl = (MemberImpl) member;
                        final Address memberAddress = memberImpl.getAddress();
                        if (!memberImpl.localMember() && node.connectionManager.getOrConnect(memberAddress) == null) {
                            allConnected = false;
                            systemLogService.logJoin("Not-connected to " + memberAddress);
                        }
                    }
                }
            }
            if (!node.joined() || !allConnected) {
                if (Clock.currentTimeMillis() - getStartTime() < maxJoinMillis) {
                    logger.warning("Failed to connect, node joined= " + node.joined() + ", allConnected= "
                            + allConnected + " to all other members after " + checkCount + " seconds.");
                    logger.warning("Rebooting after 10 seconds.");
                    try {
                        Thread.sleep(REBOOT_WAIT_MILLIS);
                        node.rejoin();
                    } catch (InterruptedException e) {
                        logger.warning(e);
                        node.shutdown(false);
                    }
                } else {
                    throw new HazelcastException("Failed to join in "
                            + TimeUnit.MILLISECONDS.toSeconds(maxJoinMillis) + " seconds!");
                }
                return;
            }
        }
        if (node.getClusterService().getSize() == 1) {
            final StringBuilder sb = new StringBuilder("\n");
            sb.append(node.clusterService.membersString());
            logger.info(sb.toString());
        }
    }

    protected void failedJoiningToMaster(boolean multicast, int tryCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("======================================================");
        sb.append("\n");
        sb.append("Couldn't connect to discovered master! tryCount: ").append(tryCount);
        sb.append("\n");
        sb.append("address: ").append(node.getThisAddress());
        sb.append("\n");
        sb.append("masterAddress: ").append(node.getMasterAddress());
        sb.append("\n");
        sb.append("multicast: ").append(multicast);
        sb.append("\n");
        sb.append("connection: ").append(node.connectionManager.getConnection(node.getMasterAddress()));
        sb.append("\n");
        sb.append("======================================================");
        sb.append("\n");
        throw new IllegalStateException(sb.toString());
    }

    boolean shouldMerge(JoinMessage joinRequest) {
        boolean shouldMerge = false;
        if (joinRequest != null) {
            boolean validJoinRequest;
            try {
                try {
                    validJoinRequest = node.getClusterService().validateJoinMessage(joinRequest);
                } catch (Exception e) {
                    logger.finest(e.getMessage());
                    validJoinRequest = false;
                }
                if (!validJoinRequest) {
                    return false;
                }
                for (Member member : node.getClusterService().getMembers()) {
                    MemberImpl memberImpl = (MemberImpl) member;
                    if (memberImpl.getAddress().equals(joinRequest.getAddress())) {
                        logIfFinestEnabled(logger, "Should not merge to " + joinRequest.getAddress()
                                + ", because it is already member of this cluster.");
                        return false;
                    }
                }
                int currentMemberCount = node.getClusterService().getMembers().size();
                if (joinRequest.getMemberCount() > currentMemberCount) {
                    // I should join the other cluster
                    logger.info(node.getThisAddress() + " is merging to " + joinRequest.getAddress()
                            + ", because : joinRequest.getMemberCount() > currentMemberCount ["
                            + (joinRequest.getMemberCount() + " > " + currentMemberCount) + "]");
                    logIfFinestEnabled(logger, joinRequest.toString());
                    shouldMerge = true;
                } else if (joinRequest.getMemberCount() == currentMemberCount) {
                    // compare the hashes
                    if (node.getThisAddress().hashCode() > joinRequest.getAddress().hashCode()) {
                        logger.info(node.getThisAddress() + " is merging to " + joinRequest.getAddress()
                                + ", because : node.getThisAddress().hashCode() > joinRequest.address.hashCode() "
                                + ", this node member count: " + currentMemberCount);
                        logIfFinestEnabled(logger, joinRequest.toString());
                        shouldMerge = true;
                    } else {
                        logIfFinestEnabled(logger, joinRequest.getAddress() + " should merge to this node "
                                + ", because : node.getThisAddress().hashCode() < joinRequest.address.hashCode() "
                                + ", this node member count: " + currentMemberCount);
                    }
                }
            } catch (Throwable e) {
                logger.severe(e);
                return false;
            }
        }
        return shouldMerge;
    }

    protected void connectAndSendJoinRequest(Collection<Address> colPossibleAddresses) {
        for (Address possibleAddress : colPossibleAddresses) {
            final Connection conn = node.connectionManager.getOrConnect(possibleAddress);
            if (conn != null) {
                logIfFinestEnabled(logger, "sending join request for " + possibleAddress);
                node.clusterService.sendJoinRequest(possibleAddress, true);
            }
        }
    }

    @Override
    public void reset() {
        joinStartTime.set(Clock.currentTimeMillis());
        tryCount.set(0);
    }

    protected void startClusterMerge(final Address targetAddress) {
        final OperationService operationService = node.nodeEngine.getOperationService();
        final Collection<MemberImpl> memberList = node.getClusterService().getMemberList();
        final Collection<Future> calls = new ArrayList<Future>();
        for (MemberImpl member : memberList) {
            if (!member.localMember()) {
                Future f = operationService.createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                        new PrepareMergeOperation(targetAddress), member.getAddress())
                        .setTryCount(3).invoke();
                calls.add(f);
            }
        }
        for (Future f : calls) {
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.finest("While waiting merge response...", e);
            }
        }
        final PrepareMergeOperation prepareMergeOperation = new PrepareMergeOperation(targetAddress);
        prepareMergeOperation.setNodeEngine(node.nodeEngine).setService(node.getClusterService())
                .setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
        operationService.runOperationOnCallingThread(prepareMergeOperation);


        for (MemberImpl member : memberList) {
            if (!member.localMember()) {
                operationService.createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                        new MergeClustersOperation(targetAddress), member.getAddress())
                        .setTryCount(1).invoke();
            }
        }

        final MergeClustersOperation mergeClustersOperation = new MergeClustersOperation(targetAddress);
        mergeClustersOperation.setNodeEngine(node.nodeEngine).setService(node.getClusterService())
                .setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
        operationService.runOperationOnCallingThread(mergeClustersOperation);
    }

    @Override
    public final long getStartTime() {
        return joinStartTime.get();
    }

    @Override
    public void setTargetAddress(Address targetAddress) {
        this.targetAddress = targetAddress;
    }

    public Address getTargetAddress() {
        final Address target = targetAddress;
        targetAddress = null;
        return target;
    }
}
