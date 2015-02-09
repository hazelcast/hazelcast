/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class providing static factory methods that create write behind queues.
 */
public final class WriteBehindQueues {

    private WriteBehindQueues() {
    }

    public static WriteBehindQueue createBoundedWriteBehindQueue(int maxCapacity, AtomicInteger counter) {
        final WriteBehindQueue queue = createCyclicWriteBehindQueue();
        final WriteBehindQueue boundedQueue = createBoundedWriteBehindQueue(maxCapacity, counter, queue);
        return createSyncronizedWriteBehindQueue(boundedQueue);
    }

    public static <T> WriteBehindQueue<T> createDefaultWriteBehindQueue() {
        final WriteBehindQueue queue = createCoalescedWriteBehindQueue();
        return createSyncronizedWriteBehindQueue(queue);
    }

    private static WriteBehindQueue createSyncronizedWriteBehindQueue(WriteBehindQueue queue) {
        return new SynchronizedWriteBehindQueue(queue);
    }

    private static WriteBehindQueue createCoalescedWriteBehindQueue() {
        return new CoalescedWriteBehindQueue();
    }

    private static WriteBehindQueue createCyclicWriteBehindQueue() {
        return new CyclicWriteBehindQueue();
    }

    private static WriteBehindQueue createBoundedWriteBehindQueue(int maxCapacity, AtomicInteger counter,
                                                                  WriteBehindQueue queue) {
        return new BoundedWriteBehindQueue(maxCapacity, counter, queue);
    }

}
