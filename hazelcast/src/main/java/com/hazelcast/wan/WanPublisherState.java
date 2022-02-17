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

package com.hazelcast.wan;

import com.hazelcast.wan.impl.WanReplicationService;

/**
 * Defines the state in which a WAN publisher can be in if it is not shutting
 * down.
 */
public enum WanPublisherState {
    /**
     * State where both enqueuing new events is allowed, enqueued events are
     * replicated to the target cluster and WAN sync is enabled.
     * The publisher is most often in REPLICATING state when the target cluster
     * is operational.
     */
    REPLICATING((byte) 0, true, true),
    /**
     * State where new events are enqueued but they are not dequeued. Some events
     * which have been dequeued before the state was switched may still be
     * replicated to the target cluster but further events will not be
     * replicated. WAN sync is enabled.
     * For instance, this state may be useful if you know that the target cluster
     * is temporarily unavailable (is under maintenance) and that the WAN queues
     * can hold as many events as is necessary to reconcile the state between
     * two clusters once the target cluster becomes available.
     */
    PAUSED((byte) 1, true, false),
    /**
     * State where neither new events are enqueued nor dequeued. As with the
     * {@link #PAUSED} state, some events might still be replicated after the
     * publisher has switched to this state. WAN sync is enabled.
     * For instance, this state may be useful if you know that the target cluster
     * is being shut down, decomissioned and being put out of use and that it
     * will never come back. In such cases, you may additionally clear the WAN
     * queues to release the consumed heap after the publisher has been switched
     * into this state.
     * An another example would be starting a publisher in STOPPED state. This
     * may be the case where you know that the target cluster is not initially
     * available and will be unavailable for a definite period but at some point
     * it will become available. Once it becomes available, you can then switch
     * the publisher state to REPLICATING to begin replicating to that cluster.
     *
     * @see WanReplicationService#removeWanEvents(String, String)
     */
    STOPPED((byte) 2, false, false);

    private static final WanPublisherState[] STATE_VALUES = values();
    private final boolean enqueueNewEvents;
    private final boolean replicateEnqueuedEvents;
    private final byte id;

    WanPublisherState(byte id,
                      boolean enqueueNewEvents,
                      boolean replicateEnqueuedEvents) {
        this.id = id;
        this.enqueueNewEvents = enqueueNewEvents;
        this.replicateEnqueuedEvents = replicateEnqueuedEvents;
    }

    /**
     * Returns the WanPublisherState as an enum.
     */
    public static WanPublisherState getByType(final byte id) {
        for (WanPublisherState state : STATE_VALUES) {
            if (state.id == id) {
                return state;
            }
        }
        return null;
    }

    /**
     * Returns {@code true} if this state allows enqueueing new events,
     * {@code false} otherwise.
     */
    public boolean isEnqueueNewEvents() {
        return enqueueNewEvents;
    }

    /**
     * Returns {@code true} if this state allows dequeueing and replicating
     * events, {@code false} otherwise.
     */
    public boolean isReplicateEnqueuedEvents() {
        return replicateEnqueuedEvents;
    }

    /**
     * Returns the ID of the WAN publisher state.
     */
    public byte getId() {
        return id;
    }
}
