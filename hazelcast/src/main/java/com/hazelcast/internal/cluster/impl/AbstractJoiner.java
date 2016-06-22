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
import com.hazelcast.cluster.Joiner;
import com.hazelcast.config.Config;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.operations.JoinCheckOperation;
import com.hazelcast.internal.cluster.impl.operations.MemberRemoveOperation;
import com.hazelcast.internal.cluster.impl.operations.MergeClustersOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.FutureUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;

public abstract class AbstractJoiner implements Joiner {

    private static final int JOIN_TRY_COUNT = 5;
    private static final long MIN_WAIT_SECONDS_BEFORE_JOIN = 10;
    private static final long SPLIT_BRAIN_CONN_TIMEOUT = 5000;
    private static final long SPLIT_BRAIN_SLEEP_TIME = 10;
    private static final int SPLIT_BRAIN_JOIN_CHECK_TIMEOUT_SECONDS = 10;
    private static final int SPLIT_BRAIN_MERGE_TIMEOUT_SECONDS = 30;

    protected final Config config;
    protected final Node node;
    protected final ClusterServiceImpl clusterService;
    protected final ILogger logger;

    // map blacklisted endpoints. Boolean value represents if blacklist is temporary or permanent
    protected final ConcurrentMap<Address, Boolean> blacklistedAddresses = new ConcurrentHashMap<Address, Boolean>();
    protected final ClusterJoinManager clusterJoinManager;

    private final AtomicLong joinStartTime = new AtomicLong(Clock.currentTimeMillis());
    private final AtomicInteger tryCount = new AtomicInteger(0);

    private final long mergeNextRunDelayMs;
    private volatile Address targetAddress;

    private final FutureUtil.ExceptionHandler splitBrainMergeExceptionHandler = new FutureUtil.ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable instanceof MemberLeftException) {
                return;
            }
            logger.warning("Problem while waiting for merge operation result", throwable);
        }
    };


    public AbstractJoiner(Node node) {
        this.node = node;
        this.logger = node.loggingService.getLogger(getClass());
        this.config = node.config;
        this.clusterService = node.getClusterService();
        this.clusterJoinManager = clusterService.getClusterJoinManager();
        mergeNextRunDelayMs = node.getProperties().getMillis(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS);
    }

    @Override
    public final long getStartTime() {
        return joinStartTime.get();
    }

    @Override
    public void setTargetAddress(Address targetAddress) {
        this.targetAddress = targetAddress;
    }

    @Override
    public void blacklist(Address address, boolean permanent) {
        logger.info(address + " is added to the blacklist.");
        blacklistedAddresses.putIfAbsent(address, permanent);
    }

    @Override
    public boolean unblacklist(Address address) {
        if (blacklistedAddresses.remove(address, Boolean.FALSE)) {
            logger.info(address + " is removed from the blacklist.");
            return true;
        }
        return false;
    }

    @Override
    public boolean isBlacklisted(Address address) {
        return blacklistedAddresses.containsKey(address);
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

        if (logger.isFineEnabled()) {
            logger.fine("PostJoin master: " + node.getMasterAddress() + ", isMaster: " + node.isMaster());
        }
        if (node.getState() != NodeState.ACTIVE) {
            return;
        }
        if (tryCount.incrementAndGet() == JOIN_TRY_COUNT) {
            logger.warning("Join try count exceed limit, setting this node as master!");
            node.setAsMaster();
        }

        if (node.joined()) {
            if (!node.isMaster()) {
                ensureConnectionToAllMembers();
            }

            if (clusterService.getSize() == 1) {
                logger.info('\n' + node.clusterService.membersString());
            }
        }
    }

    private void ensureConnectionToAllMembers() {
        boolean allConnected = false;
        if (node.joined()) {
            logger.fine("Waiting for all connections");
            int connectAllWaitSeconds = node.getProperties().getSeconds(GroupProperty.CONNECT_ALL_WAIT_SECONDS);
            int checkCount = 0;
            while (checkCount++ < connectAllWaitSeconds && !allConnected) {
                try {
                    //noinspection BusyWait
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ignored) {
                    EmptyStatement.ignore(ignored);
                }

                allConnected = true;
                Collection<Member> members = clusterService.getMembers();
                for (Member member : members) {
                    if (!member.localMember() && node.connectionManager.getOrConnect(member.getAddress()) == null) {
                        allConnected = false;
                        if (logger.isFineEnabled()) {
                            logger.fine("Not-connected to " + member.getAddress());
                        }
                    }
                }
            }
        }
    }

    protected final long getMaxJoinMillis() {
        return node.getProperties().getMillis(GroupProperty.MAX_JOIN_SECONDS);
    }

    protected final long getMaxJoinTimeToMasterNode() {
        // max join time to found master node,
        // this should be significantly greater than MAX_WAIT_SECONDS_BEFORE_JOIN property
        // hence we add 10 seconds more
        return TimeUnit.SECONDS.toMillis(MIN_WAIT_SECONDS_BEFORE_JOIN)
                + node.getProperties().getMillis(GroupProperty.MAX_WAIT_SECONDS_BEFORE_JOIN);
    }

    @SuppressWarnings({"checkstyle:methodlength", "checkstyle:returncount",
            "checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    protected boolean shouldMerge(JoinMessage joinMessage) {
        if (joinMessage == null) {
            return false;
        }

        try {
            boolean validJoinRequest = clusterJoinManager.validateJoinMessage(joinMessage);
            if (!validJoinRequest) {
                logger.fine("Cannot process split brain merge message from " + joinMessage.getAddress()
                        + ", since join-message could not be validated.");
                return false;
            }
        } catch (Exception e) {
            logger.log(Level.FINE, "failure during validating join message", e);
            return false;
        }

        try {
            if (clusterService.getMember(joinMessage.getAddress()) != null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Should not merge to " + joinMessage.getAddress()
                            + ", because it is already member of this cluster.");
                }
                return false;
            }

            final ClusterState clusterState = clusterService.getClusterState();
            if (clusterState != ClusterState.ACTIVE) {
                if (logger.isFineEnabled()) {
                    logger.fine("Should not merge to " + joinMessage.getAddress() + ", because this cluster is in "
                            + clusterState + " state.");
                }
                return false;
            }

            Collection<Address> targetMemberAddresses = joinMessage.getMemberAddresses();
            if (targetMemberAddresses.contains(node.getThisAddress())) {
                node.nodeEngine.getOperationService()
                        .send(new MemberRemoveOperation(node.getThisAddress()), joinMessage.getAddress());
                logger.info(node.getThisAddress() + " CANNOT merge to " + joinMessage.getAddress()
                        + ", because it thinks this-node as its member.");
                return false;
            }

            Collection<Address> thisMemberAddresses = clusterService.getMemberAddresses();
            for (Address address : thisMemberAddresses) {
                if (targetMemberAddresses.contains(address)) {
                    logger.info(node.getThisAddress() + " CANNOT merge to " + joinMessage.getAddress()
                            + ", because it thinks " + address + " as its member. "
                            + "But " + address + " is member of this cluster.");
                    return false;
                }
            }

            int targetDataMemberCount = joinMessage.getDataMemberCount();
            int currentDataMemberCount = clusterService.getSize(DATA_MEMBER_SELECTOR);

            if (targetDataMemberCount > currentDataMemberCount) {
                // I should join the other cluster
                logger.info(node.getThisAddress() + " is merging to " + joinMessage.getAddress()
                        + ", because : joinMessage.getDataMemberCount() > currentDataMemberCount ["
                        + (targetDataMemberCount + " > " + currentDataMemberCount) + ']');
                if (logger.isFineEnabled()) {
                    logger.fine(joinMessage.toString());
                }
                return true;
            } else if (targetDataMemberCount == currentDataMemberCount) {
                // compare the hashes
                if (node.getThisAddress().hashCode() > joinMessage.getAddress().hashCode()) {
                    logger.info(node.getThisAddress() + " is merging to " + joinMessage.getAddress()
                            + ", because : node.getThisAddress().hashCode() > joinMessage.address.hashCode() "
                            + ", this node member count: " + currentDataMemberCount);
                    if (logger.isFineEnabled()) {
                        logger.fine(joinMessage.toString());
                    }
                    return true;
                } else {
                    logger.info(joinMessage.getAddress() + " should merge to this node "
                            + ", because : node.getThisAddress().hashCode() < joinMessage.address.hashCode() "
                            + ", this node data member count: " + currentDataMemberCount);
                }
            } else {
                logger.info(joinMessage.getAddress() + " should merge to this node "
                        + ", because : currentDataMemberCount > joinMessage.getDataMemberCount() ["
                        + (currentDataMemberCount + " > " + targetDataMemberCount) + ']');
            }
        } catch (Throwable e) {
            logger.severe(e);
        }
        return false;
    }

    protected JoinMessage sendSplitBrainJoinMessage(Address target) {
        if (logger.isFineEnabled()) {
            logger.fine(node.getThisAddress() + " is connecting to " + target);
        }

        Connection conn = node.connectionManager.getOrConnect(target, true);
        long timeout = SPLIT_BRAIN_CONN_TIMEOUT;
        while (conn == null) {
            timeout -= SPLIT_BRAIN_SLEEP_TIME;
            if (timeout < 0) {
                return null;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(SPLIT_BRAIN_SLEEP_TIME);
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
                return null;
            }
            conn = node.connectionManager.getConnection(target);
        }

        NodeEngine nodeEngine = node.nodeEngine;
        Future future = nodeEngine.getOperationService().createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                new JoinCheckOperation(node.createSplitBrainJoinMessage()), target)
                .setTryCount(1).invoke();
        try {
            return (JoinMessage) future.get(SPLIT_BRAIN_JOIN_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logger.log(Level.FINE, "Timeout during join check!", e);
        } catch (Exception e) {
            logger.warning("Error during join check!", e);
        }
        return null;
    }

    @Override
    public void reset() {
        joinStartTime.set(Clock.currentTimeMillis());
        tryCount.set(0);
    }

    protected void startClusterMerge(final Address targetAddress) {
        ClusterServiceImpl clusterService = node.clusterService;

        if (!prepareClusterState(clusterService)) {
            return;
        }

        OperationService operationService = node.nodeEngine.getOperationService();
        Collection<Member> memberList = clusterService.getMembers();
        Collection<Future> futures = new ArrayList<Future>(memberList.size());
        for (Member member : memberList) {
            if (!member.localMember()) {
                Operation op = new MergeClustersOperation(targetAddress);
                Future<Object> future =
                        operationService.invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, member.getAddress());
                futures.add(future);
            }
        }

        waitWithDeadline(futures, SPLIT_BRAIN_MERGE_TIMEOUT_SECONDS, TimeUnit.SECONDS, splitBrainMergeExceptionHandler);

        Operation mergeClustersOperation = new MergeClustersOperation(targetAddress);
        mergeClustersOperation.setNodeEngine(node.nodeEngine).setService(clusterService)
                .setOperationResponseHandler(createEmptyResponseHandler());
        operationService.runOperationOnCallingThread(mergeClustersOperation);
    }

    private boolean prepareClusterState(ClusterServiceImpl clusterService) {
        if (!preCheckClusterState(clusterService)) {
            return false;
        }

        long until = Clock.currentTimeMillis() + mergeNextRunDelayMs;
        while (clusterService.getClusterState() == ClusterState.ACTIVE) {
            try {
                clusterService.changeClusterState(ClusterState.FROZEN);
                return true;
            } catch (Exception e) {
                String error = e.getClass().getName() + ": " + e.getMessage();
                logger.warning("While freezing cluster state! " + error);
            }

            if (Clock.currentTimeMillis() >= until) {
                logger.warning("Could not change cluster state to FROZEN in time. "
                        + "Postponing merge process until next attempt.");
                return false;
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                logger.warning("Interrupted while preparing cluster for merge!");
                // restore interrupt flag
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private boolean preCheckClusterState(final ClusterService clusterService) {
        final ClusterState initialState = clusterService.getClusterState();
        if (initialState != ClusterState.ACTIVE) {
            logger.warning("Could not prepare cluster state since it has been changed to " + initialState);
            return false;
        }

        return true;
    }

    protected Address getTargetAddress() {
        final Address target = targetAddress;
        targetAddress = null;
        return target;
    }
}
