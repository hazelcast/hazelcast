/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Cluster;
import com.hazelcast.instance.NodeState;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

/**
 * {@code ClusterState} consists several states of the cluster
 * which each state can allow and/or deny specific actions
 * and/or change behaviours of specific actions.
 * <p/>
 * There are 5 states:
 * <ol>
 * <li>
 * {@link #ACTIVE}:
 * Cluster will continue to operate without any restriction.
 * </li>
 * <li>
 * {@link #NO_MIGRATION}:
 * Migrations (partition rebalancing) and backup replications
 * are not allowed. Cluster will continue to operate without any restriction.
 * </li>
 * <li>
 * {@link #FROZEN}:
 * New members are not allowed to join, partition table/assignments will be frozen.
 * All other operations are allowed and will operate without any restriction.
 * If some members leave the cluster during it is in {@code FROZEN} state, they can join back.
 * </li>
 * <li>
 * {@link #PASSIVE}:
 * New members are not allowed to join.
 * All operations, except the ones marked with {@link AllowedDuringPassiveState},
 * will be rejected immediately.
 * </li>
 * <li>
 * {@link #IN_TRANSITION}:
 * Shows that {@code ClusterState} is in transition.
 * This is a temporary & intermediate state, not allowed to set explicitly.
 * </li>
 * </ol>
 * <p/>
 * By default, cluster will be in {@code ACTIVE} state. During split-brain merge process,
 * state of the cluster, that is going to join to the major side,
 * will be changed to {@code FROZEN} automatically before merge
 * and will be set to the state of the new cluster after merge.
 *
 * @see Cluster#getClusterState()
 * @see Cluster#changeClusterState(ClusterState)
 * @see NodeState
 * @since 3.6
 */
public enum ClusterState {

    /**
     * In {@code ACTIVE} state, cluster will continue to operate without any restriction.
     * All operations are allowed. This is default state of a cluster.
     */
    ACTIVE(true, true, true),

    /**
     * In {@code NO_MIGRATION} state of the cluster, migrations (partition rebalancing) and backup replications
     * are not allowed.
     * <ul>
     * <li>
     * When a new member joins, it will not be assigned any partitions until cluster state changes to {@link ClusterState#ACTIVE}.
     * </li>
     * <li>
     * When a member leaves, backups of primary replicas owned by that member will be promoted to primary.
     * But missing backup replicas will not be created/replicated until cluster state changes to {@link ClusterState#ACTIVE}.
     * </li>
     * </ul>
     *
     * Cluster will continue to operate without any restriction. All operations are allowed.
     *
     * @since 3.9
     */
    NO_MIGRATION(true, false, true),

    /**
     * In {@code FROZEN} state of the cluster:
     * <ul>
     * <li>
     * New members are not allowed to join, except the members left during {@code FROZEN} or {@link ClusterState#PASSIVE} state.
     * For example, cluster has 3 nodes; A, B and C in {@code FROZEN} state. If member B leaves
     * the cluster (either proper shutdown or crash), it will be allowed to re-join to the cluster.
     * But another member D, won't be able to join.
     * </li>
     * <li>
     * Partition table/assignments will be frozen. When a member leaves the cluster, its partition
     * assignments (as primary and backup) will remain the same, until either that member re-joins
     * to the cluster or {@code ClusterState} changes back to {@code ACTIVE}.
     * If that member re-joins in {@code FROZEN}, it will own all previous partition assignments as it is.
     * If {@code ClusterState} changes to {@code ACTIVE} then partition re-balancing process will
     * kick-in and all unassigned partitions will be assigned to active members.
     * It's not allowed to change {@code ClusterState} to {@code FROZEN}
     * when there are pending migration/replication tasks in the system.
     * </li>
     * <li>
     * Nodes continue to stay in {@link NodeState#ACTIVE} state when cluster goes into the {@code FROZEN} state.
     * </li>
     * <li>
     * All other operations except migrations are allowed and will operate without any restriction.
     * </li>
     * </ul>
     */
    FROZEN(false, false, false),

    /**
     * In {@code PASSIVE} state of the cluster:
     * <ul>
     * <li>
     * New members are not allowed to join, except the members left during {@link ClusterState#FROZEN} or {@code PASSIVE} state.
     * </li>
     * * <li>
     * Partition table/assignments will be frozen. It's not allowed to change {@code ClusterState}
     * to {@code PASSIVE} when there are pending migration/replication tasks in the system. If some
     * nodes leave the cluster while cluster is in {@code PASSIVE} state, they will be removed from the
     * partition table if cluster state moves back to {@link #ACTIVE}.
     * </li>
     * <li>
     * When cluster state is moved to {@code PASSIVE}, node are moved to {@link NodeState#PASSIVE}.
     * In this regard, when cluster state moves to another state from {@code PASSIVE}, nodes become
     * {@link NodeState#ACTIVE}.
     * </li>
     * <li>
     * All operations, except the ones marked with {@link AllowedDuringPassiveState},
     * will be rejected immediately.
     * </li>
     * </ul>
     */
    PASSIVE(false, false, false),

    /**
     * Shows that ClusterState is in transition. When a state change transaction is started,
     * ClusterState will be shown as {@code IN_TRANSITION} during the transaction lifecycle.
     * After transaction completes, it will be in either new state or its previous state
     * depending on transaction's result.
     * <p/>
     * This is a temporary & intermediate state, not allowed to set explicitly.
     * <p/>
     * <ul>
     * <li>
     * Similar to the {@code FROZEN} state, new members are not allowed
     * and migration/replication process will be paused.
     * </li>
     * <li>
     * If membership change occurs in the cluster, cluster state transition will fail
     * and will be reverted back to the previous state.
     * </li>
     * </ul>
     */
    IN_TRANSITION(false, false, false);

    private final boolean joinAllowed;
    private final boolean migrationAllowed;
    private final boolean partitionPromotionAllowed;

    ClusterState(boolean joinAllowed, boolean migrationAllowed, boolean partitionPromotionAllowed) {
        this.joinAllowed = joinAllowed;
        this.migrationAllowed = migrationAllowed;
        this.partitionPromotionAllowed = partitionPromotionAllowed;
    }

    /**
     * Shows whether new member join is allowed.
     * @return true when join is allowed, false otherwise
     */
    public boolean isJoinAllowed() {
        return joinAllowed;
    }

    /**
     * Shows whether migrations and replications are allowed.
     * @return true when migrations and replications are allowed, false otherwise
     */
    public boolean isMigrationAllowed() {
        return migrationAllowed;
    }

    /**
     * Shows whether partition promotions are allowed.
     * @return true when partition promotions are allowed, false otherwise
     */
    public boolean isPartitionPromotionAllowed() {
        return partitionPromotionAllowed;
    }
}
