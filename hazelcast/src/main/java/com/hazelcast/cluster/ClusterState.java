/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

/**
 * {@code ClusterState} consists several states of the cluster
 * which each state can allow and/or deny specific actions
 * and/or change behaviours of specific actions.
 * <p/>
 * There are 4 states:
 * <ol>
 * <li>
 * {@link #ACTIVE}:
 * Cluster will continue to operate without any restriction.
 * </li>
 * <li>
 * {@link #FROZEN}:
 * New members are not allowed to join, partition table/assignments will be frozen.
 * All other operations are allowed and will operate without any restriction.
 * </li>
 * <li>
 * {@link #SHUTTING_DOWN}:
 * New members are not allowed to join.
 * All operations, except the ones marked with {@link com.hazelcast.spi.impl.AllowedDuringShutdown},
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
 * @see com.hazelcast.core.Cluster#getClusterState()
 * @see com.hazelcast.core.Cluster#changeClusterState(ClusterState)
 * @since 3.6
 */
public enum ClusterState {

    /**
     * In {@code ACTIVE} state, cluster will continue to operate without any restriction.
     * All operations are allowed. This is default state of a cluster.
     */
    ACTIVE,

    /**
     * In {@code FROZEN} state of the cluster:
     * <ul>
     * <li>
     * New members are not allowed to join, except the members left during {@code FROZEN} state.
     * For example, cluster has 3 nodes; A, B and C in {@code FROZEN} state.
     * If member B leaves the cluster (either proper shutdown or crash),
     * it will be allowed to re-join to the cluster. But another member D, won't be
     * able to join.
     * </li>
     * <li>
     * Partition table/assignments will be frozen. When a member leaves the cluster,
     * its partition assignments (as primary and backup) will remain the same,
     * until either that member re-joins to the cluster
     * or {@code ClusterState} changes back to {@code ACTIVE}.
     * If that member re-joins in {@code FROZEN}, it will own all previous partition assignments as it is.
     * If {@code ClusterState} changes to {@code ACTIVE} then partition re-balancing process
     * will kick-in and all unassigned partitions will be assigned to active members.
     * It's not allowed to change {@code ClusterState} to {@code FROZEN}
     * when there are pending migration/replication tasks in the system.
     * </li>
     * <li>
     * All other operations are allowed and will operate without any restriction.
     * </li>
     * </ul>
     */
    FROZEN,

    /**
     * In {@code SHUTTING_DOWN} state of the cluster:
     * <ul>
     * <li>
     * New members are not allowed to join, even the ones left during {@code SHUTTING_DOWN} state.
     * </li>
     * <li>
     * Partition table/assignments will be frozen. It's not allowed to change {@code ClusterState}
     * to {@code SHUTTING_DOWN} when there are pending migration/replication tasks in the system.
     * </li>
     * <li>
     * All operations, except the ones marked with {@link com.hazelcast.spi.impl.AllowedDuringShutdown},
     * will be rejected immediately.
     * </li>
     * <li>
     * It's not allowed to go back any other state, once {@code ClusterState} is changed to
     * {@code SHUTTING_DOWN}. From now on, only possible/allowed action is shutting down the instance.
     * </li>
     * </ul>
     */
    SHUTTING_DOWN,

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
    IN_TRANSITION

}
