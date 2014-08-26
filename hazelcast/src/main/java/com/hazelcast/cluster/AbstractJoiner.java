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
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.SystemLogService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;

public abstract class AbstractJoiner implements Joiner {
    private static final ExceptionHandler WHILE_WAIT_MERGE_EXCEPTION_HANDLER =
            logAllExceptions("While waiting merge response...", Level.FINEST);

    private final AtomicLong joinStartTime = new AtomicLong(Clock.currentTimeMillis());
    private final AtomicInteger tryCount = new AtomicInteger(0);
    protected final Set<Address> blacklistedAddresses
            = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());
    protected final Config config;
    protected final Node node;
    protected final ILogger logger;
    protected final SystemLogService systemLogService;

    private volatile Address targetAddress;

    public AbstractJoiner(Node node) {
        this.node = node;
        this.systemLogService = node.getSystemLogService();
        this.logger = node.loggingService.getLogger(getClass());
        this.config = node.config;
    }

    @Override
    public void blacklist(Address callerAddress) {
        logger.info(callerAddress + " is added to the blacklist.");
        blacklistedAddresses.add(callerAddress);
    }

    @Override
    public boolean isBlacklisted(Address address) {
        return blacklistedAddresses.contains(address);
    }

    public abstract void doJoin();

    @Override
    public final void join() {
        blacklistedAddresses.clear();
        doJoin();
        postJoin();
    }

    private void postJoin() {
        blacklistedAddresses.clear();

        systemLogService.logJoin("PostJoin master: " + node.getMasterAddress() + ", isMaster: " + node.isMaster());
        if (!node.isActive()) {
            return;
        }
        if (tryCount.incrementAndGet() == 5) {
            logger.warning("Join try count exceed limit, setting this node as master!");
            node.setAsMaster();
        }

        if (node.joined()) {
            if (!node.isMaster()) {
                ensureConnectionToAllMembers();
            }

            if (node.getClusterService().getSize() == 1) {
                final StringBuilder sb = new StringBuilder("\n");
                sb.append(node.clusterService.membersString());
                logger.info(sb.toString());
            }
        }
    }

    private void ensureConnectionToAllMembers() {
        boolean allConnected = false;
        if (node.joined()) {
            systemLogService.logJoin("Waiting for all connections");
            int connectAllWaitSeconds = node.groupProperties.CONNECT_ALL_WAIT_SECONDS.getInteger();
            int checkCount = 0;
            while (checkCount++ < connectAllWaitSeconds && !allConnected) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }

                allConnected = true;
                Collection<MemberImpl> members = node.getClusterService().getMemberList();
                for (MemberImpl member : members) {
                    if (!member.localMember() && node.connectionManager.getOrConnect(member.getAddress()) == null) {
                        allConnected = false;
                        systemLogService.logJoin("Not-connected to " + member.getAddress());
                    }
                }
            }
        }
    }

    protected final long getMaxJoinMillis() {return node.getGroupProperties().MAX_JOIN_SECONDS.getInteger() * 1000L;}

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
                if (validJoinRequest) {
                    for (Member member : node.getClusterService().getMembers()) {
                        MemberImpl memberImpl = (MemberImpl) member;
                        if (memberImpl.getAddress().equals(joinRequest.getAddress())) {
                            if (logger.isFinestEnabled()) {
                                logger.finest("Should not merge to " + joinRequest.getAddress()
                                        + ", because it is already member of this cluster.");
                            }
                            return false;
                        }
                    }
                    int currentMemberCount = node.getClusterService().getMembers().size();
                    if (joinRequest.getMemberCount() > currentMemberCount) {
                        // I should join the other cluster
                        logger.info(node.getThisAddress() + " is merging to " + joinRequest.getAddress()
                                + ", because : joinRequest.getMemberCount() > currentMemberCount ["
                                + (joinRequest.getMemberCount() + " > " + currentMemberCount) + "]");
                        if (logger.isFinestEnabled()) {
                            logger.finest(joinRequest.toString());
                        }
                        shouldMerge = true;
                    } else if (joinRequest.getMemberCount() == currentMemberCount) {
                        // compare the hashes
                        if (node.getThisAddress().hashCode() > joinRequest.getAddress().hashCode()) {
                            logger.info(node.getThisAddress() + " is merging to " + joinRequest.getAddress()
                                    + ", because : node.getThisAddress().hashCode() > joinRequest.address.hashCode() "
                                    + ", this node member count: " + currentMemberCount);
                            if (logger.isFinestEnabled()) {
                                logger.finest(joinRequest.toString());
                            }
                            shouldMerge = true;
                        } else {
                            if (logger.isFinestEnabled()) {
                                logger.finest(joinRequest.getAddress() + " should merge to this node "
                                        + ", because : node.getThisAddress().hashCode() < joinRequest.address.hashCode() "
                                        + ", this node member count: " + currentMemberCount);
                            }
                        }
                    }
                }
            } catch (Throwable e) {
                logger.severe(e);
                return false;
            }
        }
        return shouldMerge;
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
                Operation operation = new PrepareMergeOperation(targetAddress);
                Future f = operationService.createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                        operation, member.getAddress()).setTryCount(3).invoke();
                calls.add(f);
            }
        }

        try {
            waitWithDeadline(calls, 1, TimeUnit.SECONDS, WHILE_WAIT_MERGE_EXCEPTION_HANDLER);
        } catch (TimeoutException e) {
            logger.warning("While waiting merge response...", e);
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
