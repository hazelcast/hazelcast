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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;

/**
 * Factory for the queue implementations used by write-behind map stores.
 * <p>
 * The returned queues are decorator stacks:
 * <ul>
 *     <li>write-coalescing: synchronized -> coalesced</li>
 *     <li>non-write-coalescing: synchronized -> bounded -> cyclic</li>
 * </ul>
 */
public final class WriteBehindQueues {

    private WriteBehindQueues() {
    }

    /**
     * Creates the queue used when write coalescing is enabled.
     */
    public static WriteBehindQueue<DelayedEntry> createDefaultWriteBehindQueue() {
        WriteBehindQueue<DelayedEntry> coalescedQueue = createCoalescedWriteBehindQueue();
        return synchronize(coalescedQueue);
    }

    /**
     * Creates the queue used when write coalescing is disabled.
     */
    public static WriteBehindQueue<DelayedEntry> createBoundedWriteBehindQueue(MapStoreContext mapStoreContext) {
        NodeWideUsedCapacityCounter capacityCounter =
                mapStoreContext.getMapServiceContext().getNodeWideUsedCapacityCounter();
        return createBoundedWriteBehindQueue(capacityCounter);
    }

    /**
     * Creates the queue used when write coalescing is disabled.
     */
    public static WriteBehindQueue<DelayedEntry> createBoundedWriteBehindQueue(NodeWideUsedCapacityCounter counter) {
        WriteBehindQueue<DelayedEntry> cyclicQueue = createCyclicWriteBehindQueue();
        WriteBehindQueue<DelayedEntry> boundedQueue = createBoundedWriteBehindQueue(cyclicQueue, counter);
        return synchronize(boundedQueue);
    }

    static WriteBehindQueue<DelayedEntry> createCoalescedWriteBehindQueue() {
        return new CoalescedWriteBehindQueue();
    }

    static <T extends DelayedEntry> WriteBehindQueue<T> createBoundedWriteBehindQueue(WriteBehindQueue<T> queue,
                                                                                      NodeWideUsedCapacityCounter counter) {
        return new BoundedWriteBehindQueue<>(queue, counter);
    }

    static WriteBehindQueue<DelayedEntry> createCyclicWriteBehindQueue() {
        return new CyclicWriteBehindQueue();
    }

    private static <T> WriteBehindQueue<T> synchronize(WriteBehindQueue<T> queue) {
        return new SynchronizedWriteBehindQueue<>(queue);
    }
}
