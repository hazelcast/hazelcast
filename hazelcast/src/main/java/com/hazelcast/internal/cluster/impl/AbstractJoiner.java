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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.config.Config;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage.SplitBrainMergeCheckResult;
import com.hazelcast.internal.cluster.impl.operations.MergeClustersOp;
import com.hazelcast.internal.cluster.impl.operations.SplitBrainMergeValidationOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.FutureUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.cluster.ClusterState.IN_TRANSITION;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static java.lang.Thread.currentThread;

public abstract class AbstractJoiner
        implements Joiner {

    private static final int JOIN_TRY_COUNT = 5;
    private static final int SPLIT_BRAIN_MERGE_TIMEOUT_SECONDS = 30;
    private static final int SPLIT_BRAIN_JOIN_CHECK_TIMEOUT_SECONDS = 10;
    private static final long MIN_WAIT_BEFORE_JOIN_SECONDS = 10;
    private static final long SPLIT_BRAIN_SLEEP_TIME_MILLIS = 10;
    private static final long SPLIT_BRAIN_CONN_TIMEOUT_MILLIS = 5000;

    protected final Config config;
    protected final Node node;
    protected final ClusterServiceImpl clusterService;
    protected final ILogger logger;

    // map blacklisted endpoints. Boolean value represents if blacklist is temporary or permanent
    protected final ConcurrentMap<Address, Boolean> blacklistedAddresses = new ConcurrentHashMap<>();
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
        this.mergeNextRunDelayMs = node.getProperties().getMillis(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS);
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
        Boolean prev = blacklistedAddresses.putIfAbsent(address, permanent);
        if (prev == null) {
            logger.info(address + " is " + (permanent ? "permanently " : "") + "added to the blacklist.");
        }
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
        if (!clusterService.isJoined() && isMemberExcludedFromHotRestart()) {
            logger.warning("Could not join to the cluster because hot restart data must be reset.");
            node.getNodeExtension().getInternalHotRestartService().forceStartBeforeJoin();
            reset();
            doJoin();
        }
        postJoin();
    }

    protected final boolean shouldRetry() {
        return node.isRunning() && !clusterService.isJoined() && !isMemberExcludedFromHotRestart();
    }

    private boolean isMemberExcludedFromHotRestart() {
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
        if (clusterService.isJoined()) {
            logger.fine("Waiting for all connections");
            int connectAllWaitSeconds = node.getProperties().getSeconds(ClusterProperty.CONNECT_ALL_WAIT_SECONDS);
            int checkCount = 0;
            while (checkCount++ < connectAllWaitSeconds) {
                boolean allConnected = true;
                Collection<Member> members = clusterService.getMembers();
                for (Member member : members) {
                    if (!member.localMember() && node.getServer().getConnectionManager(MEMBER)
                            .getOrConnect(member.getAddress()) == null) {
                        allConnected = false;
                        if (logger.isFineEnabled()) {
                            logger.fine("Not-connected to " + member.getAddress());
                        }
                    }
                }
                if (allConnected) {
                    break;
                }
                try {
                    //noinspection BusyWait
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ignored) {
                    currentThread().interrupt();
                }
            }
        }
    }

    protected final long getMaxJoinMillis() {
        return node.getProperties().getMillis(ClusterProperty.MAX_JOIN_SECONDS);
    }

    protected final long getMaxJoinTimeToMasterNode() {
        // max join time to found master node,
        // this should be significantly greater than MAX_WAIT_SECONDS_BEFORE_JOIN property
        // hence we add 10 seconds more
        return TimeUnit.SECONDS.toMillis(MIN_WAIT_BEFORE_JOIN_SECONDS)
                + node.getProperties().getMillis(ClusterProperty.MAX_WAIT_SECONDS_BEFORE_JOIN);
    }

    /**
     * Sends a split brain join request to the target address and checks the response to see if this node should merge
     * to the target address.
     */
    protected final SplitBrainMergeCheckResult sendSplitBrainJoinMessageAndCheckResponse(Address target,
                                                                                         SplitBrainJoinMessage request) {
        SplitBrainJoinMessage response = sendSplitBrainJoinMessage(target, request);
        return clusterService.getClusterJoinManager().shouldMerge(response);
    }

    /**
     * Sends a split brain join request to the target address and returns the response.
     */
    private SplitBrainJoinMessage sendSplitBrainJoinMessage(Address target, SplitBrainJoinMessage request) {
        if (logger.isFineEnabled()) {
            logger.fine("Sending SplitBrainJoinMessage to " + target);
        }

        Connection conn = node.getServer().getConnectionManager(MEMBER).getOrConnect(target, true);
        long timeout = SPLIT_BRAIN_CONN_TIMEOUT_MILLIS;
        while (conn == null) {
            timeout -= SPLIT_BRAIN_SLEEP_TIME_MILLIS;
            if (timeout < 0) {
                logger.fine("Returning null timeout<0, " + timeout);
                return null;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(SPLIT_BRAIN_SLEEP_TIME_MILLIS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                return null;
            }
            conn = node.getServer().getConnectionManager(MEMBER).get(target);
        }

        NodeEngine nodeEngine = node.nodeEngine;
        Future future = nodeEngine.getOperationService().createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                new SplitBrainMergeValidationOp(request), target)
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

    protected void startClusterMerge(Address targetAddress, int expectedMemberListVersion) {
        ClusterServiceImpl clusterService = node.clusterService;

        if (!prepareClusterState(clusterService, expectedMemberListVersion)) {
            return;
        }

        OperationService operationService = node.nodeEngine.getOperationService();
        Collection<Member> memberList = clusterService.getMembers();
        Collection<Future> futures = new ArrayList<>(memberList.size());
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
     * The method will keep trying to change the cluster state until {@link ClusterProperty#MERGE_NEXT_RUN_DELAY_SECONDS} elapses
     * or until the sleep period between two attempts has been interrupted.
     *
     * @param clusterService the cluster service used for state change
     * @return true if the cluster state was successfully prepared
     */
    private boolean prepareClusterState(ClusterServiceImpl clusterService, int expectedMemberListVersion) {
        if (!preCheckClusterState(clusterService)) {
            return false;
        }

        long until = Clock.currentTimeMillis() + mergeNextRunDelayMs;
        while (Clock.currentTimeMillis() < until) {
            ClusterState clusterState = clusterService.getClusterState();
            if (!clusterState.isMigrationAllowed() && !clusterState.isJoinAllowed() && clusterState != IN_TRANSITION) {
                return (clusterService.getMemberListVersion() == expectedMemberListVersion);
            }

            if (clusterService.getMemberListVersion() != expectedMemberListVersion) {
                logger.warning("Could not change cluster state to FROZEN because local member list version: "
                        + clusterService.getMemberListVersion() + " is different than expected member list version: "
                        + expectedMemberListVersion);
                return false;
            }

            // If state is IN_TRANSITION, then skip trying to change state.
            // Otherwise transaction will print noisy warning logs.
            if (clusterState != IN_TRANSITION) {
                try {
                    clusterService.changeClusterState(FROZEN);

                    return verifyMemberListVersionAfterStateChange(clusterService, clusterState, expectedMemberListVersion);
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

        logger.warning("Could not change cluster state to FROZEN in time. Postponing merge process until next attempt.");
        return false;
    }

    private boolean verifyMemberListVersionAfterStateChange(ClusterServiceImpl clusterService, ClusterState clusterState,
                                                            int expectedMemberListVersion) {
        if (clusterService.getMemberListVersion() != expectedMemberListVersion) {
            try {
                logger.warning("Reverting cluster state back to " + clusterState + " because member list version: "
                        + clusterService.getMemberListVersion() + " is different than expected member list version: "
                        + expectedMemberListVersion);
                clusterService.changeClusterState(clusterState);
            } catch (Exception e) {
                String error = e.getClass().getName() + ": " + e.getMessage();
                logger.warning("While reverting cluster state to " + clusterState + "! " + error);
            }

            return false;
        }

        return true;
    }

    /**
     * Returns true if the current cluster state allows join; either
     * {@link ClusterState#ACTIVE} or {@link ClusterState#NO_MIGRATION}.
     */
    private boolean preCheckClusterState(ClusterService clusterService) {
        ClusterState initialState = clusterService.getClusterState();
        if (!initialState.isJoinAllowed()) {
            logger.warning("Could not prepare cluster state since it has been changed to " + initialState);
            return false;
        }

        return true;
    }

    protected Address getTargetAddress() {
        Address target = targetAddress;
        targetAddress = null;
        return target;
    }
}
