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
 * Receives updates about the context in which Hazelcast is executed
 * in order to detect what is the intent of topology changes from the
 * runtime environment that may affect how the Hazelcast cluster should react.
 */
public interface ClusterTopologyIntentTracker {

    int UNKNOWN = -1;

    /**
     * Process an update of the cluster topology. Each update carries a triple of information about
     * the previous & current requested cluster size and the current number of ready members in the cluster.
     * <br/>
     * <b>Examples</b>
     * <p>
     *     A cluster with requested size 3 is starting up. This is expected to result in the following series
     *     of updates:
     *     <pre>{@code
     *     (-1, 3, 0)
     *     (3, 3, 1)
     *     (3, 3, 2)
     *     (3, 3, 3)
     *     }</pre>
     * </p>
     * <p>
     *     Assuming user requests scaling up a running cluster of 3 members to 5, the following
     *     updates are expected:
     *     <pre>{@code
     *     (3, 3, 3)
     *     (3, 5, 3)
     *     (3, 5, 4)
     *     (3, 5, 5)
     *     }</pre>
     * </p>
     * Notice that actual updates may differ (eg duplicate notifications of intermediate states may be received).
     *
     * @param previousClusterSpecSize   previously requested cluster size
     * @param currentClusterSpecSize    currently requested cluster size
     * @param readyNodesCount           number of members that currently ready and participate in the cluster.
     * @param currentNodesCount
     *
     * @see NodeExtension#isReady()
     */
    void update(int previousClusterSpecSize, int currentClusterSpecSize, int readyNodesCount,
                int currentNodesCount);

    ClusterTopologyIntent getClusterTopologyIntent();

    /**
     * Initialize explicitly the cluster topology intent.
     * @param clusterTopologyIntent
     */
    void initializeClusterTopologyIntent(ClusterTopologyIntent clusterTopologyIntent);

    /**
     * Handle Hazelcast node shutdown with the given cluster topology intent.
     * @param clusterTopologyIntent
     */
    void shutdownWithIntent(ClusterTopologyIntent clusterTopologyIntent);

    /**
     * @return {@code true} if this instance of {@code ClusterTopologyIntentTracker} is active and tracking
     *         cluster topology changes in a managed context, otherwise {@code false}.
     */
    boolean isEnabled();

    /**
     * @return  the number of requested Hazelcast members in the cluster, as determined by the specification
     *          that is managed by the runtime context. When running Hazelcast in a Kubernetes StatefulSet,
     *          this corresponds to the value in {@code StatefulSetSpec.size}.
     */
    int getCurrentClusterSpecSize();

    /**
     * Notifies the {@link ClusterTopologyIntentTracker} that Hazelcast members list has changed.
     */
    void onMembershipChange();

    /**
     * @return  {@code true} when this tracker accepts events coalesced, exactly as received from Kubernetes API,
     *          or {@code false} when coalesced events should be split into separate updates.
     *
     * @implNote see also {@code KubernetesClient$StsMonitor#onMessage} method for implementation details
     */
    boolean acceptsCoalescedEvents();
}
