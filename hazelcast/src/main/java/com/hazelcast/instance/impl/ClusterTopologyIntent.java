/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
 * {@link #NOT_IN_MANAGED_CONTEXT} indicates Hazelcast is not executed within a
 * managed runtime context; all other values represent different detected intents
 * within a managed runtime context.
 *
 * @see ClusterTopologyIntentTracker
 */
public enum ClusterTopologyIntent {
    /**
     * Hazelcast is not deployed in a managed context
     */
    NOT_IN_MANAGED_CONTEXT(0),
    /**
     * Unknown state, but in managed context (Kubernetes)
     */
    IN_MANAGED_CONTEXT_UNKNOWN(1),
    /**
     * No change to the number of Hazelcast members in the cluster is intended
     */
    CLUSTER_STABLE(2),
    /**
     * No change to the number of Hazelcast members in the cluster is intended,
     * but some members are missing from the cluster.
     * For example this might happen when kubernetes reschedules a pod, so it is first shutdown, then restarted.
     * Even though the requested number of members in the cluster stays the same
     * all the time, a member will be missing for some time and the detected intent during
     * that time will be {@code MISSING_MEMBERS}.
     */
    CLUSTER_STABLE_WITH_MISSING_MEMBERS(3),
    /**
     * Full cluster shutdown is intended
     */
    CLUSTER_SHUTDOWN(4),
    /**
     * Cluster is starting up
     */
    CLUSTER_START(5),
    /**
     * Cluster is shutting down, but had some missing members before
     * cluster-wide shutdown.
     */
    CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS(6),
    /**
     * Hazelcast cluster is being scaled up or down
     */
    SCALING(7);

    private final int id;

    ClusterTopologyIntent(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static ClusterTopologyIntent of(int id) {
        for (ClusterTopologyIntent intent : ClusterTopologyIntent.values()) {
            if (intent.id == id) {
                return intent;
            }
        }
        throw new IllegalArgumentException("No ClusterTopologyIntent exists with id " + id);
    }
}
