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

package com.hazelcast.instance.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.utils.RetryUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.spi.properties.ClusterProperty.PERSISTENCE_AUTO_CLUSTER_STATE_STRATEGY;
import static java.lang.Thread.sleep;

/**
 * Implementation of {@link ClusterTopologyIntentTracker} that automates cluster state management
 * according to intent of topology changes detected in kubernetes environment.
 * <br/>
 * Example flow of change in detected intent and associated {@code currentClusterSpecSize} on a cluster's master member:
 * (User action on left, detected intent with cluster spec size in parenthese on the right side).
 * <pre>
 * {@code
 * +-----------------------------------------------------------------+---------------------------------+
 * | $ helm install hz --set cluster.memberCount=3 \                 | NOT_IN_MANAGED_CONTEXT(-1) ->   |
 * |             hazelcast-enterprise-5.3.1-gcs.tgz                  | CLUSTER_START(3) ->             |
 * |                                                                 | (after pods are started)        |
 * |                                                                 | STABLE(3)                       |
 * +-----------------------------------------------------------------+---------------------------------+
 * | $ kubectl scale sts hz-hazelcast-enterprise --replicas 5        | SCALING(5) ->                   |
 * |                                                                 | (after new pods are started)    |
 * |                                                                 | STABLE(5)                       |
 * +-----------------------------------------------------------------+---------------------------------+
 * | $ kubectl delete pod hz-hazelcast-enterprise-2                  | MISSING_MEMBERS(5) ->           |
 * |   (simulating kubernetes deleted a pod)                         | (after pod is restarted)        |
 * |                                                                 | STABLE(5)                       |
 * +-----------------------------------------------------------------+---------------------------------+
 * | $ kubectl scale sts hz-hazelcast-enterprise --replicas 0        | CLUSTER_SHUTDOWN(0)             |
 * +-----------------------------------------------------------------+---------------------------------+
 * }
 * </pre>
 */
public class ClusterTopologyIntentTrackerImpl implements ClusterTopologyIntentTracker {

    /**
     * Currently detected cluster topology intent.
     */
    private final AtomicReference<ClusterTopologyIntent> clusterTopologyIntent =
            new AtomicReference<>(ClusterTopologyIntent.NOT_IN_MANAGED_CONTEXT);
    // single-threaded executor for actions in response to cluster topology intent changes
    private final ExecutorService clusterTopologyExecutor;
    // applies when automatic cluster state management is enabled (with persistence in kubernetes)
    private final ClusterState clusterStateForMissingMembers;
    private final ILogger logger;
    private final Node node;
    /**
     * The desired number of members, as specified in the runtime environment. eg in kubernetes
     * {@code kubectl scale sts hz --replicas 5} means {@code currentClusterSpecSize} is 5.
     */
    private volatile int currentClusterSpecSize = UNKNOWN;
    /**
     * The last known cluster spec size while cluster was detected in {@link ClusterTopologyIntent#CLUSTER_STABLE} intent.
     * Used during cluster-wide shutdown, while {@link #currentClusterSpecSize} is {@code 0}.
     */
    private volatile int lastKnownStableClusterSpecSize = UNKNOWN;
    /**
     * Current Hazelcast cluster size, as observed by {@link com.hazelcast.internal.cluster.ClusterService}.
     */
    private volatile int currentClusterSize;

    public ClusterTopologyIntentTrackerImpl(Node node) {
        this.clusterStateForMissingMembers = node.getProperties()
                .getEnum(PERSISTENCE_AUTO_CLUSTER_STATE_STRATEGY, ClusterState.class);
        if (clusterStateForMissingMembers != ClusterState.FROZEN
                && clusterStateForMissingMembers != ClusterState.NO_MIGRATION) {
            throw new InvalidConfigurationException("Value of property " + PERSISTENCE_AUTO_CLUSTER_STATE_STRATEGY.getName()
                    + " was " + clusterStateForMissingMembers + " but should be one of FROZEN, NO_MIGRATION.");
        }
        this.clusterTopologyExecutor = Executors.newSingleThreadExecutor();
        this.logger = node.getLogger(ClusterTopologyIntentTracker.class);
        this.node = node;
    }

    public void shutdown() {
        clusterTopologyExecutor.shutdown();
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    public void update(int previousClusterSpecSize, int currentClusterSpecSize, int readyNodesCount, int currentNodesCount) {
        final int previousClusterSpecSizeValue = this.currentClusterSpecSize;
        this.currentClusterSpecSize = currentClusterSpecSize;
        if (previousClusterSpecSize == UNKNOWN) {
            if (currentClusterSpecSize > 0
                    && (readyNodesCount == UNKNOWN || readyNodesCount == 0)) {
                // startup of first member of new cluster
                logger.info("Cluster starting in managed context");
                clusterTopologyIntent.set(ClusterTopologyIntent.CLUSTER_START);
            } else {
                logger.info("Member starting in managed context");
                clusterTopologyIntent.set(ClusterTopologyIntent.IN_MANAGED_CONTEXT_UNKNOWN);
            }
            return;
        }
        final ClusterTopologyIntent previous = clusterTopologyIntent.get();
        ClusterTopologyIntent newTopologyIntent;
        if (currentClusterSpecSize == 0) {
            if (previousClusterSpecSizeValue > 0) {
                this.lastKnownStableClusterSpecSize = previousClusterSpecSizeValue;
            }
            newTopologyIntent = nextIntentWhenShuttingDown(previous);
        } else if (previousClusterSpecSize == currentClusterSpecSize) {
            if (ignoreUpdateWhenClusterSpecEqual(previous, readyNodesCount)) {
                return;
            }
            newTopologyIntent = nextIntentWhenClusterSpecEqual(previous, readyNodesCount, currentNodesCount);
        } else {
            newTopologyIntent = ClusterTopologyIntent.SCALING;
        }
        if (clusterTopologyIntent.compareAndSet(previous, newTopologyIntent)) {
            logger.info("Cluster topology intent: " + previous + " -> " + newTopologyIntent);
            clusterTopologyExecutor.submit(() -> {
                node.getNodeExtension().getInternalHotRestartService().onClusterTopologyIntentChange();
                if (!node.isMaster()) {
                    return;
                }
                if (newTopologyIntent == ClusterTopologyIntent.SCALING) {
                    changeClusterState(ClusterState.ACTIVE);
                } else if (newTopologyIntent == ClusterTopologyIntent.CLUSTER_STABLE) {
                    if (getClusterService().getClusterState() != ClusterState.ACTIVE) {
                        executeOrScheduleClusterStateChange(ClusterState.ACTIVE);
                    } else if (!getPartitionService().isPartitionTableHealthy()) {
                        getPartitionService().getMigrationManager().triggerControlTask();
                    }
                }
            });
        }
    }

    private ClusterTopologyIntent nextIntentWhenShuttingDown(ClusterTopologyIntent previous) {
        // if members were previously missing, next intent is CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS
        // otherwise plain CLUSTER_SHUTDOWN
        return previous == ClusterTopologyIntent.CLUSTER_STABLE_WITH_MISSING_MEMBERS
                || previous == ClusterTopologyIntent.CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS
                ? ClusterTopologyIntent.CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS
                : ClusterTopologyIntent.CLUSTER_SHUTDOWN;
    }

    /**
     * Decide whether an update from managed context should be ignored when cluster spec size stays the same.
     * @return {@code true} if update should be ignored, otherwise {@code false}.
     */
    private boolean ignoreUpdateWhenClusterSpecEqual(ClusterTopologyIntent previous,
                                                     int readyNodesCount) {
        if (readyNodesCount != currentClusterSpecSize
            && (previous == ClusterTopologyIntent.SCALING
                || previous == ClusterTopologyIntent.IN_MANAGED_CONTEXT_UNKNOWN
                || previous == ClusterTopologyIntent.CLUSTER_START)) {
            logger.info("Ignoring update because readyNodesCount is "
                    + readyNodesCount + ", while spec requires " + currentClusterSpecSize
                    + " and previous cluster topology intent is" + previous);
            return true;
        }
        return false;
    }

    /**
     * @return next {@code ClusterTopologyIntent} according to rules applicable while there is no change
     * in cluster size specification in managed context. (ie StatefulSetSpec.size remains the same, so
     * user does not intend a change in cluster size).
     */
    private ClusterTopologyIntent nextIntentWhenClusterSpecEqual(ClusterTopologyIntent previous,
                                                                 int readyNodesCount,
                                                                 int currentNodesCount) {
        if (readyNodesCount == currentClusterSpecSize && previous != ClusterTopologyIntent.CLUSTER_STABLE) {
            return ClusterTopologyIntent.CLUSTER_STABLE;
        } else if (previous == ClusterTopologyIntent.CLUSTER_STABLE && currentNodesCount < currentClusterSpecSize) {
            return ClusterTopologyIntent.CLUSTER_STABLE_WITH_MISSING_MEMBERS;
        } else {
            return previous;
        }
    }

    @Override
    public ClusterTopologyIntent getClusterTopologyIntent() {
        return clusterTopologyIntent.get();
    }

    @Override
    public void initializeClusterTopologyIntent(ClusterTopologyIntent clusterTopologyIntent) {
        ClusterTopologyIntent current = this.clusterTopologyIntent.get();
        logger.info("Current node cluster topology intent is " + current);
        // if not UNKNOWN, then it was already initialized
        if (current == ClusterTopologyIntent.IN_MANAGED_CONTEXT_UNKNOWN) {
            logger.info("Initializing this node's cluster topology to " + clusterTopologyIntent);
            this.clusterTopologyIntent.set(clusterTopologyIntent);
        }
    }

    @Override
    public void shutdownWithIntent(ClusterTopologyIntent shutdownIntent) {
        // consider the detected shutdown intent before triggering node shutdown
        if (shutdownIntent == ClusterTopologyIntent.CLUSTER_STABLE
                || shutdownIntent == ClusterTopologyIntent.CLUSTER_STABLE_WITH_MISSING_MEMBERS) {
            try {
                // wait for partition table to be healthy before switching to NO_MIGRATION
                // eg in "rollout restart" case, node is shutdown in NO_MIGRATION state
                executeWithShutdownTimeout(() -> getPartitionService().isPartitionTableHealthy());
                changeClusterState(clusterStateForMissingMembers);
            } catch (Throwable t) {
                // let shutdown proceed even though we failed to switch to FROZEN state
                logger.warning("Could not switch to transient FROZEN state while cluster"
                        + "shutdown intent was " + shutdownIntent, t);
            }
        } else if (shutdownIntent == ClusterTopologyIntent.CLUSTER_SHUTDOWN) {
            clusterWideShutdown();
        } else if (shutdownIntent == ClusterTopologyIntent.CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS) {
            // if cluster is shutting down with missing members, it is possible that a member might
            // rejoin while attempting the graceful shutdown. In this case, races may occur, which may
            // lead to lack of progress on the shutting-down member.
            waitForMissingMember();
            clusterWideShutdownWithMissingMember(shutdownIntent);
        }
    }

    private void clusterWideShutdownWithMissingMember(ClusterTopologyIntent shutdownIntent) {
        try {
            // The do-while loop ensures that we wait for partition table to be healthy after successfully
            // switching to PASSIVE cluster state (during which attempts of missing members to rejoin will
            // be denied), otherwise we retry by switching to ACTIVE cluster state.
            do {
                logger.info("Waiting for partition table to be healthy");
                if (!getPartitionService().isPartitionTableHealthy()) {
                    logger.warning("Switching to ACTIVE state in order to allow for partition table to be healthy");
                    changeClusterState(ClusterState.ACTIVE);
                    executeWithShutdownTimeout(() -> getPartitionService().isPartitionTableHealthy());
                }
                changeClusterState(ClusterState.PASSIVE);
            } while (!getPartitionService().isPartitionTableHealthy());
        } catch (Throwable t) {
            // let shutdown proceed even though we failed to switch to PASSIVE state
            // and wait for replica sync
            logger.warning("Could not switch to transient PASSIVE state while cluster"
                    + "shutdown intent was " + shutdownIntent, t);
        }
        long timeoutNanos = node.getProperties().getNanos(ClusterProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS);
        try {
            getNodeExtension().getInternalHotRestartService()
                    .waitPartitionReplicaSyncOnCluster(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (IllegalStateException e) {
            logger.severe("Failure while waiting for partition replica sync before shutdown", e);
        }
    }

    private void clusterWideShutdown() {
        try {
            changeClusterState(ClusterState.PASSIVE);
        } catch (Throwable t) {
            // let shutdown proceed even though we failed to switch to PASSIVE state
            logger.warning("Could not switch to transient PASSIVE state while cluster "
                    + "shutdown intent was CLUSTER_SHUTDOWN.", t);
        }
        long timeoutNanos = node.getProperties().getNanos(ClusterProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS);
        try {
            // wait for replica sync
            getNodeExtension().getInternalHotRestartService()
                    .waitPartitionReplicaSyncOnCluster(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (IllegalStateException e) {
            logger.severe("Failure while waiting for partition replica sync before shutdown.", e);
        }
    }

    private void waitForMissingMember() {
        if (getClusterService().getClusterState() == ClusterState.PASSIVE) {
            // cluster is already in PASSIVE state and shutting down, so don't wait
            return;
        }
        if (lastKnownStableClusterSpecSize == currentClusterSize) {
            return;
        }
        logger.info("Waiting for missing members: lastKnownStableClusterSpecSize: " + lastKnownStableClusterSpecSize + ", "
                + "currentClusterSize " + currentClusterSize);
        executeWithShutdownTimeout(() -> lastKnownStableClusterSpecSize == currentClusterSize);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public int getCurrentClusterSpecSize() {
        return currentClusterSpecSize;
    }

    @Override
    public void onMembershipChange() {
        currentClusterSize = getClusterService().getSize();
    }

    @Override
    public boolean acceptsCoalescedEvents() {
        return clusterStateForMissingMembers == ClusterState.FROZEN;
    }

    private void executeOrScheduleClusterStateChange(ClusterState newClusterState) {
        if (!getNodeExtension().getInternalHotRestartService().setDeferredClusterState(newClusterState)) {
            // hot restart recovery is completed, just apply the new cluster state here
            changeClusterState(newClusterState);
        }
    }

    /**
     *
     * @param callable  {@link Callable<Boolean>} that returns {@code true} when its condition is completed and
     *                  control should return to caller.
     * @return {@code true} if completed because callable completed normally or {@code false} if timeout passed or
     *         thread was interrupted.
     */
    private boolean executeWithShutdownTimeout(Callable<Boolean> callable) {
        boolean callableCompleted;
        long timeoutNanos = node.getProperties().getNanos(ClusterProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS);
        while (true) {
            try {
                callableCompleted = callable.call();
                if (callableCompleted || timeoutNanos < 0) break;
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
            try {
                sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            timeoutNanos -= TimeUnit.SECONDS.toNanos(1);
        }
        return callableCompleted;
    }

    /**
     * Change cluster state, if current state is not already the desired one.
     * Retries up to 3 times. The cluster state change is transient, so if persistence
     * is enabled, the new cluster state is not persisted to disk.
     *
     * @param newClusterState
     */
    private void changeClusterState(ClusterState newClusterState) {
        RetryUtils.retry(
                () -> {
                    getClusterService().changeClusterState(newClusterState, true);
                    return null;
                }, 3);
    }

    private NodeExtension getNodeExtension() {
        return node.getNodeExtension();
    }

    private InternalPartitionServiceImpl getPartitionService() {
        return node.partitionService;
    }

    private ClusterServiceImpl getClusterService() {
        return node.clusterService;
    }
}
