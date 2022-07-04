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
         * Fired when a client is connected to a new cluster.
         */
        CLIENT_CHANGED_CLUSTER
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
        if (!(o instanceof LifecycleEvent)) {
            return false;
        }

        LifecycleEvent that = (LifecycleEvent) o;
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
