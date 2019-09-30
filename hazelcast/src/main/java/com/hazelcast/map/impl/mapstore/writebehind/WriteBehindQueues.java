/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;

/**
 * A class providing static factory methods that create write behind queues.
 */
public final class WriteBehindQueues {

    private WriteBehindQueues() {
    }

    public static WriteBehindQueue<DelayedEntry> createBoundedWriteBehindQueue(NodeWideUsedCapacityCounter counter) {
        final WriteBehindQueue<DelayedEntry> queue = createCyclicWriteBehindQueue();
        final WriteBehindQueue<DelayedEntry> boundedQueue = createBoundedWriteBehindQueue(queue, counter);
        return createSynchronizedWriteBehindQueue(boundedQueue);
    }

    public static WriteBehindQueue<DelayedEntry> createDefaultWriteBehindQueue() {
        final WriteBehindQueue<DelayedEntry> queue = createCoalescedWriteBehindQueue();
        return createSynchronizedWriteBehindQueue(queue);
    }

    static WriteBehindQueue<DelayedEntry> createCoalescedWriteBehindQueue() {
        return new CoalescedWriteBehindQueue();
    }

    static <T> WriteBehindQueue<T> createBoundedWriteBehindQueue(WriteBehindQueue<T> queue,
                                                                 NodeWideUsedCapacityCounter nodeWideUsedCapacityCounter) {
        return new BoundedWriteBehindQueue<>(queue, nodeWideUsedCapacityCounter);
    }

    private static <T> WriteBehindQueue<T> createSynchronizedWriteBehindQueue(WriteBehindQueue<T> queue) {
        return new SynchronizedWriteBehindQueue<>(queue);
    }

    private static WriteBehindQueue<DelayedEntry> createCyclicWriteBehindQueue() {
        return new CyclicWriteBehindQueue();
    }
}
