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
import com.hazelcast.cluster.Joiner;
import com.hazelcast.config.Config;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.operations.MergeClustersOp;
import com.hazelcast.internal.cluster.impl.operations.SplitBrainMergeValidationOp;
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
        if (!clusterService.isJoined() && shouldResetHotRestartData()) {
            logger.warning("Could not join to the cluster because hot restart data must be reset.");
            node.getNodeExtension().getInternalHotRestartService().resetHotRestartData();
            reset();
            doJoin();
        }
        postJoin();
    }

    protected final boolean shouldRetry() {
        return node.isRunning() && !clusterService.isJoined() && !shouldResetHotRestartData();
    }

    private boolean shouldResetHotRestartData() {
        final NodeExtension nodeExtension = node.getNodeExtension();
        return !nodeExtension.isStartCompleted()
                && nodeExtension.getInternalHotRestartService().isMemberExcluded(node.getThisAddress(), node.getThisUuid());
    }

    private void postJoin() {
        blacklistedAddresses.clear();

        if (logger.isFineEnabled()) {
            logger.fine("PostJoin master: " + clusterService.getMasterAddress() + ", isMaster: " + clusterService.isMaster());
        }
        if (!node.isRunning()) {
            return;
        }
        if (tryCount.incrementAndGet() == JOIN_TRY_COUNT) {
            logger.warning("Join try count exceed limit, setting this node as master!");
            clusterJoinManager.setThisMemberAsMaster();
        }

        if (clusterService.isJoined()) {
            if (!clusterService.isMaster()) {
                ensureConnectionToAllMembers();
            }

            if (clusterService.getSize() == 1) {
                clusterService.printMemberList();
            }
        }
    }

    private void ensureConnectionToAllMembers() {
        boolean allConnected = false;
        if (clusterService.isJoined()) {
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

    @SuppressWarnings({"checkstyle:returncount", "checkstyle:npathcomplexity"})
    protected boolean shouldMerge(SplitBrainJoinMessage joinMessage) {
        if (logger.isFineEnabled()) {
            logger.fine("Should merge to: " + joinMessage);
        }

        if (joinMessage == null) {
            return false;
        }

        if (!checkValidSplitBrainJoinMessage(joinMessage)) {
            return false;
        }

        if (!checkCompatibleSplitBrainJoinMessage(joinMessage)) {
            return false;
        }

        if (!checkMergeTargetIsNotMember(joinMessage)) {
            return false;
        }

        if (!checkClusterStateAllowsJoinBeforeMerge(joinMessage)) {
            return false;
        }

        if (!checkMembershipIntersectionSetEmpty(joinMessage)) {
            return false;
        }

        int targetDataMemberCount = joinMessage.getDataMemberCount();
        int currentDataMemberCount = clusterService.getSize(DATA_MEMBER_SELECTOR);

        if (targetDataMemberCount > currentDataMemberCount) {
            logger.info("We are merging to " + joinMessage.getAddress()
                    + ", because their data member count is bigger than ours ["
                    + (targetDataMemberCount + " > " + currentDataMemberCount) + ']');
            return true;
        }

        if (targetDataMemberCount < currentDataMemberCount) {
            logger.info(joinMessage.getAddress() + " should merge to us "
                    + ", because our data member count is bigger than theirs ["
                    + (currentDataMemberCount + " > " + targetDataMemberCount) + ']');
            return false;
        }

        // targetDataMemberCount == currentDataMemberCount
        if (shouldMergeTo(node.getThisAddress(), joinMessage.getAddress())) {
            logger.info("We are merging to " + joinMessage.getAddress()
                    + ", both have the same data member count: " + currentDataMemberCount);
            return true;
        }

        logger.info(joinMessage.getAddress() + " should merge to us "
                + ", both have the same data member count: " + currentDataMemberCount);
        return false;
    }

    private boolean checkValidSplitBrainJoinMessage(SplitBrainJoinMessage joinMessage) {
        try {
            if (!clusterJoinManager.validateJoinMessage(joinMessage)) {
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
        if (!clusterService.getClusterVersion().equals(joinMessage.getClusterVersion())) {
            if (logger.isFineEnabled()) {
                logger.fine("Should not merge to " + joinMessage.getAddress() + " because other cluster version is "
                        + joinMessage.getClusterVersion() + " while this cluster version is "
                        + clusterService.getClusterVersion());
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

        Collection<Address> thisMemberAddresses = clusterService.getMemberAddresses();
        for (Address address : thisMemberAddresses) {
            if (targetMemberAddresses.contains(address)) {
                logger.info(node.getThisAddress() + " CANNOT merge to " + joinMessageAddress
                        + ", because it thinks " + address + " as its member. "
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
    private static boolean shouldMergeTo(Address thisAddress, Address targetAddress) {
        String thisAddressStr = "[" + thisAddress.getHost() + "]:" + thisAddress.getPort();
        String targetAddressStr = "[" + targetAddress.getHost() + "]:" + targetAddress.getPort();

        if (thisAddressStr.equals(targetAddressStr)) {
            throw new IllegalArgumentException("Addresses should be different! This: "
                    + thisAddress + ", Target: " + targetAddress);
        }

        // Since strings are guaranteed to be different, result will always be non-zero.
        int result = thisAddressStr.compareTo(targetAddressStr);
        return result > 0;
    }

    protected SplitBrainJoinMessage sendSplitBrainJoinMessage(Address target) {
        if (logger.isFineEnabled()) {
            logger.fine("Sending SplitBrainJoinMessage to " + target);
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
                new SplitBrainMergeValidationOp(node.createSplitBrainJoinMessage()), target)
                .setTryCount(1).invoke();
        try {
            return (SplitBrainJoinMessage) future.get(SPLIT_BRAIN_JOIN_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logger.fine("Timeout during join check!", e);
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
                Operation op = new MergeClustersOp(targetAddress);
                Future<Object> future =
                        operationService.invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, member.getAddress());
                futures.add(future);
            }
        }

        waitWithDeadline(futures, SPLIT_BRAIN_MERGE_TIMEOUT_SECONDS, TimeUnit.SECONDS, splitBrainMergeExceptionHandler);

        Operation op = new MergeClustersOp(targetAddress);
        op.setNodeEngine(node.nodeEngine).setService(clusterService).setOperationResponseHandler(createEmptyResponseHandler());
        operationService.run(op);
    }

    /**
     * Prepares the cluster state for cluster merge by changing it to {@link ClusterState#FROZEN}. It expects the current
     * cluster state to be {@link ClusterState#ACTIVE} or {@link ClusterState#NO_MIGRATION}.
     * The method will keep trying to change the cluster state until {@link GroupProperty#MERGE_NEXT_RUN_DELAY_SECONDS} elapses
     * or until the sleep period between two attempts has been interrupted.
     *
     * @param clusterService the cluster service used for state change
     * @return true if the cluster state was successfully prepared
     */
    private boolean prepareClusterState(ClusterServiceImpl clusterService) {
        if (!preCheckClusterState(clusterService)) {
            return false;
        }

        long until = Clock.currentTimeMillis() + mergeNextRunDelayMs;
        while (Clock.currentTimeMillis() < until) {
            ClusterState clusterState = clusterService.getClusterState();
            if (!clusterState.isMigrationAllowed() && !clusterState.isJoinAllowed()
                    && clusterState != ClusterState.IN_TRANSITION) {
                return true;
            }

            // If state is IN_TRANSITION, then skip trying to change state.
            // Otherwise transaction will print noisy warning logs.
            if (clusterState != ClusterState.IN_TRANSITION) {
                try {
                    clusterService.changeClusterState(ClusterState.FROZEN);
                    return true;
                } catch (Exception e) {
                    String error = e.getClass().getName() + ": " + e.getMessage();
                    logger.warning("While changing cluster state to FROZEN! " + error);
                }
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

        logger.warning("Could not change cluster state to FROZEN in time. "
                + "Postponing merge process until next attempt.");
        return false;
    }

    /**
     * Returns true if the current cluster state allows join; either
     * {@link ClusterState#ACTIVE} or {@link ClusterState#NO_MIGRATION}.
     */
    private boolean preCheckClusterState(final ClusterService clusterService) {
        final ClusterState initialState = clusterService.getClusterState();
        if (!initialState.isJoinAllowed()) {
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
