/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

public final class LifecycleEvent {
    public enum LifecycleState {
        STARTING,
        STARTED,
        RESTARTING,
        RESTARTED,
        PAUSING,
        PAUSED,
        RESUMING,
        RESUMED,
        SHUTTING_DOWN,
        SHUTDOWN,
        MERGING,
        MERGED,
        CLIENT_CONNECTION_LOST, // The connection to the server is lost by the client
        CLIENT_CONNECTION_OPENING, //The physical connection to the server is restored but the whole process is not finished yet.
        CLIENT_CONNECTION_OPENED;  //Client successfully finished all pre operations and is fully operable.
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
        if (this == o) return true;
        if (!(o instanceof LifecycleEvent)) return false;

        final LifecycleEvent that = (LifecycleEvent) o;

        if (state != that.state) return false;

        return true;
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
