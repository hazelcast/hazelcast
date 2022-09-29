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

/**
 * This class represents the estimated intent of topology changes
 * performed in a managed runtime context (kubernetes) that may affect the cluster.
 *
 * @see ClusterTopologyIntentTracker
 */
public enum ClusterTopologyIntent {
    /**
     * Hazelcast is not deployed in a managed context
     */
    NOT_IN_MANAGED_CONTEXT,
    /**
     * Unknown state, but in managed context (Kubernetes)
     */
    IN_MANAGED_CONTEXT_UNKNOWN,
    /**
     * No change to the number of Hazelcast members in the cluster is intended
     */
    STABLE,
    /**
     * No change to the number of Hazelcast members in the cluster is intended,
     * but some members are missing from the cluster
     */
    MISSING_MEMBERS,
    /**
     * Full cluster shutdown is intended
     */
    CLUSTER_SHUTDOWN,
    /**
     * Cluster is starting up
     */
    CLUSTER_START,
    /**
     * Cluster is shutting down, but had some missing members before
     * cluster-wide shutdown.
     */
    CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS,
    /**
     * Hazelcast cluster is being scaled up or down
     */
    SCALING,
}
