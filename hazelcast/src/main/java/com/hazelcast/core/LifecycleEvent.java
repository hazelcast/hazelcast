/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 * Lifecycle event fired when HazelcastInstance's state changes.
 * Events are fired when instance:
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
     * lifecycle states
     */
    public enum LifecycleState {
        STARTING,
        STARTED,
        SHUTTING_DOWN,
        SHUTDOWN,
        MERGING,
        MERGED,
        CLIENT_CONNECTED,
        CLIENT_DISCONNECTED
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
