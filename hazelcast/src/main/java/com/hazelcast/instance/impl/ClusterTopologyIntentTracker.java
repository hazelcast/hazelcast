/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
 * managed runtime context that may affect how the Hazelcast cluster should react.
 * <p/>
 * Terminology:
 * <ul>
 *     <li><b>Managed runtime context</b>: a runtime orchestration environment within which Hazelcast is
 *     being executed (e.g. Kubernetes).</li>
 *     <li><b>Replica</b>: a Hazelcast member that is executed in a managed runtime context. For example,
 *     when running a 3-member Hazelcast cluster, there are 3 replicas.</li>
 *     <li><b>Specification / specified replicas</b>: the desired runtime state of the Hazelcast member, as declared by the user
 *     to the managed runtime context. For example, when user executes {@code kubectl scale sts hz --replicas 5},
 *     the <b>specified replicas</b> are 5.</li>
 *     <li><b>Current replicas</b>: number of replicas that are running by the managed runtime. Notice that current replicas
 *     does not necessarily reflect the current number of members in the Hazelcast cluster. Examples:
 *     <br/>- When a kubernetes pod is terminated, "current replicas" is immediately
 *     decreased by 1, even though the Hazelcast member is still live and just started performing its graceful shutdown. So
 *     there will be some time during which current replicas may be lower than actual number of members in the Hazelcast cluster
 *     (until graceful shutdown completes).
 *     <br/>- When a kubernetes pod is scheduled, "current replicas" immediately increases by 1 even though Hazelcast is just
 *     started and not joined to any cluster.</li>
 *     <li><b>Ready replicas</b>: replicas which are observed as "ready" by the managed runtime. "Ready" implies that the
 *     managed runtime has queried the configured readiness probe and found the Hazelcast member to be ready (i.e. up & running
 *     and ready to accept and serve requests). Notice that since "ready" state is based on a periodic check by the managed
 *     runtime, its updates can lag for some time, depending on configuration (e.g. what is the period of readiness probe checks
 *     etc). Example:
 *     <br/>Kubernetes deletes a pod of a running cluster with specified replicas 3. Immediately
 *     "current replicas" drops to 2, however "ready replicas" is still 3 until the readiness probe monitoring period passes
 *     and the readiness check figures out the Hazelcast member is no longer ready.
 *     <br/>
 *     Also related to "readiness" definition: {@code NodeExtension#isReady}.</li>
 * </ul>
 */
public interface ClusterTopologyIntentTracker {

    int UNKNOWN = -1;

    /** Default value used when {@code status.*Replicas} fields are missing in the Kubernetes API response. */
    int DEFAULT_STATUS_REPLICA_COUNT = 0;

    /**
     * Process an update of the cluster topology. Each update carries information about
     * the previous & current specified replicas count, the current number of replicas and ready replicas in the cluster.
     * <br/>
     * <b>Examples</b> (numbers in parentheses indicate (previous specified replica count, current specified replica count,
     * current replicas, current ready replicas))
     * <p>
     *     A cluster with specified replica count 3 is starting up. This is expected to result in a series
     *     of updates similar to the following:
     *     <pre>{@code
     *     (-1, 3, 0, 0)
     *     (3, 3, 1, 0)
     *     (3, 3, 1, 1)
     *     (3, 3, 2, 1)
     *     (3, 3, 2, 2)
     *     (3, 3, 3, 2)
     *     (3, 3, 3, 3)
     *     }</pre>
     * </p>
     * <p>
     *     Assuming user requests scaling up a running cluster of 3 members to 5, the following
     *     updates are expected:
     *     <pre>{@code
     *     (3, 3, 3, 3)
     *     (3, 5, 4, 3)
     *     (5, 5, 4, 4)
     *     (5, 5, 5, 4)
     *     (5, 5, 5, 5)
     *     }</pre>
     * </p>
     * Notice that actual updates may differ (e.g. duplicate notifications of intermediate states may be received).
     *
     * @param previousSpecifiedReplicas   previous specified replicas count
     * @param updatedSpecifiedReplicas    updated specified replicas count
     * @param previousReadyReplicas       number of previously ready replicas
     * @param updatedReadyReplicas        number of updated ready replicas
     * @param previousCurrentReplicas     number of previous current replicas
     * @param updatedCurrentReplicas      number of updated current replicas
     *
     * @see NodeExtension#isReady()
     */
    void update(int previousSpecifiedReplicas, int updatedSpecifiedReplicas,
                int previousReadyReplicas, int updatedReadyReplicas,
                int previousCurrentReplicas, int updatedCurrentReplicas);

    ClusterTopologyIntent getClusterTopologyIntent();

    /**
     * Initialize this tracker, if the tracker supports it. This method must be called first, before the tracker
     * can receive any updates.
     */
    void initialize();

    /**
     * Prepare this tracker for shutdown.
     */
    void destroy();

    /**
     * Initialize explicitly the cluster topology intent.
     */
    void initializeClusterTopologyIntent(ClusterTopologyIntent clusterTopologyIntent);

    /**
     * Handle Hazelcast node shutdown with the given cluster topology intent.
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
    int getCurrentSpecifiedReplicaCount();

    /**
     * Notifies the {@link ClusterTopologyIntentTracker} that Hazelcast members list has changed.
     */
    void onMembershipChange();
}
