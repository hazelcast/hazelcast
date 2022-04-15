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

public enum ClusterTopologyIntent {
    /**
     * Hazelcast is not deployed in a managed context
     */
    NOT_IN_MANAGED_CONTEXT,
    /**
     * Unknown state, but in managed context (Kubernetes)
     */
    UNKNOWN,
    /**
     * No change to the number of Hazelcast members in the cluster is intended
     */
    STABLE,
    /**
     * Full cluster shutdown is intended
     */
    CLUSTER_SHUTDOWN,
    /**
     * Cluster is starting up
     */
    CLUSTER_START,
    /**
     * Hazelcast cluster is being scaled up or down
     */
    SCALING,
}
