/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

/**
 * Lifecycle events are fired when the HazelcastInstance state changes.
 * <p>
 * Events are fired when the instance is:
 * <ul>
 * <li>Starting</li>
 * <li>Started</li>
 * <li>Shutting down</li>
 * <li>Shut down completed</li>
 * <li>Merging</li>
 * <li>Merged</li>
 * <li>Client connected</li>
 * <li>Client disconnected</li>
 * <li>Client changed cluster (failover)</li>
 * <li>Client cluster id changed (cluster restarted, no failover configured)</li>
 * </ul>
 *
 * @see com.hazelcast.core.LifecycleListener
 * @see HazelcastInstance#getLifecycleService()
 */
public final class LifecycleEvent {

    /**
     * Lifecycle states
     */
    public enum LifecycleState {
        /**
         * Fired when the member is starting.
         */
        STARTING,

        /**
         * Fired when the member start is completed.
         */
        STARTED,

        /**
         * Fired when the member is shutting down.
         */
        SHUTTING_DOWN,

        /**
         * Fired when the member shut down is completed.
         */
        SHUTDOWN,

        /**
         * Fired on each cluster member just before the start of a merge
         * process into another cluster. This is typically used when a
         * split-brain situation is healed.
         */
        MERGING,

        /**
         * Fired when the merge process was successful and all data has been
         * merged.
         */
        MERGED,

        /**
         * Fired when the merge process failed for some reason.
         */
        MERGE_FAILED,

        /**
         * Fired when a client is connected to the cluster.
         */
        CLIENT_CONNECTED,

        /**
         * Fired when a client is disconnected from the cluster.
         */
        CLIENT_DISCONNECTED,

        /**
         * Fired when a client configured with failover clusters connects to a cluster
         * through the failover path. The target may be another configured cluster or a
         * restarted instance of the cluster the client was connected to, since a failover
         * client is always switched through that path when the cluster id changes.
         * Complementary to {@link #CLIENT_CLUSTER_ID_CHANGED}, which covers a client
         * without failover configuration; the two never fire together. An application
         * that must react to every cluster identity change should handle both states.
         */
        CLIENT_CHANGED_CLUSTER,

        /**
         * Fired when a client without failover configuration reconnects and finds that the
         * cluster has a different cluster id than before. This means the cluster was fully
         * shut down and started again, so any state the client registered on it, such as
         * listeners or query caches, is gone. Never fired on the first connection.
         * <p>
         * This state and {@link #CLIENT_CHANGED_CLUSTER} are complementary and never fire
         * together. A client configured with failover clusters receives
         * {@link #CLIENT_CHANGED_CLUSTER} for every cluster id change; a client without
         * failover configuration receives this state. An application that must react
         * whenever the cluster identity changes, regardless of how the client is
         * configured, should handle both states, for example with the same listener code.
         * <p>
         * The event does not carry the previous cluster id. The new one is available
         * through {@link com.hazelcast.cluster.Cluster#getClusterId()}.
         */
        CLIENT_CLUSTER_ID_CHANGED
    }

    final LifecycleState state;

    public LifecycleEvent(LifecycleState state) {
        this.state = state;
    }

    public LifecycleState getState() {
        return state;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LifecycleEvent that)) {
            return false;
        }

        return state == that.state;
    }

    @Override
    public int hashCode() {
        return state != null ? state.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "LifecycleEvent [state=" + state + "]";
    }
}
