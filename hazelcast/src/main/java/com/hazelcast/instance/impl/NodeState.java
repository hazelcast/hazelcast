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

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

/**
 * Possible states of a {@link Node} during its lifecycle.
 * Some actions/operations may be allowed or denied
 * according to current state of the {@link Node}.
 *
 * @see Node#start()
 * @see Node#shutdown(boolean)
 * @see AllowedDuringPassiveState
 * @see ClusterState
 *
 * @since 3.6
 */
public enum NodeState {

    /**
     * Basic state of the running Node. An {@code ACTIVE} node is allowed to execute/process
     * all kinds of operations. A node is in {@code ACTIVE} state while cluster state is one of
     * {@link ClusterState#ACTIVE}, {@link ClusterState#NO_MIGRATION} or {@link ClusterState#FROZEN}.
     */
    ACTIVE,

    /**
     * Node can go into the {@code PASSIVE} when one of the following things happen:
     * <ul>
     * <li>
     * When {@link Node#shutdown(boolean)} is called, until the shut down process is completed.
     * When the shut down process is completed, node goes into the {@link NodeState#SHUT_DOWN} state.
     * </li>
     * <li>
     * When the cluster state moves to {@link ClusterState#PASSIVE} via
     * {@link Cluster#changeClusterState(ClusterState)}
     * </li>
     * </ul>
     */
    PASSIVE,

    /**
     * After {@link Node#shutdown(boolean)} call completes, node's state will be {@code SHUT_DOWN}.
     * In {@code SHUT_DOWN} state node will be completely inactive. All operations/invocations
     * will be rejected. Once a node is shutdown, it cannot be restarted.
     */
    SHUT_DOWN,

    /**
     * Initial state of the node before switching to the {@link #ACTIVE} state.
     */
    STARTING
}
