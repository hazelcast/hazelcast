/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class providing static factory methods that create write behind queues.
 */
public final class WriteBehindQueues {

    private WriteBehindQueues() {
    }

    public static WriteBehindQueue<DelayedEntry> createBoundedWriteBehindQueue(int maxCapacity, AtomicInteger counter) {
        final WriteBehindQueue<DelayedEntry> queue = createCyclicWriteBehindQueue();
        final WriteBehindQueue<DelayedEntry> boundedQueue = createBoundedWriteBehindQueue(maxCapacity, counter, queue);
        return createSynchronizedWriteBehindQueue(boundedQueue);
    }

    public static WriteBehindQueue<DelayedEntry> createDefaultWriteBehindQueue() {
        final WriteBehindQueue<DelayedEntry> queue = createCoalescedWriteBehindQueue();
        return createSynchronizedWriteBehindQueue(queue);
    }

    private static <T> WriteBehindQueue<T> createSynchronizedWriteBehindQueue(WriteBehindQueue<T> queue) {
        return new SynchronizedWriteBehindQueue<T>(queue);
    }

    private static WriteBehindQueue<DelayedEntry> createCoalescedWriteBehindQueue() {
        return new CoalescedWriteBehindQueue();
    }

    private static WriteBehindQueue<DelayedEntry> createCyclicWriteBehindQueue() {
        return new CyclicWriteBehindQueue();
    }

    private static <T> WriteBehindQueue<T> createBoundedWriteBehindQueue(int maxCapacity, AtomicInteger counter,
                                                                  WriteBehindQueue<T> queue) {
        return new BoundedWriteBehindQueue<T>(maxCapacity, counter, queue);
    }
}
